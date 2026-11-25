package stream

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// ContractErrorResolver resolves a numeric Soroban contract error code for
// a contract ID into a human-readable enum variant name (e.g.
// "ContractError::NotAuthorized").
type ContractErrorResolver interface {
	ResolveContractError(ctx context.Context, contractID string, code uint32) (string, bool)
}

var contractErrorTextPattern = regexp.MustCompile(`Error\(Contract,\s*#(\d+)\)`)

// TrappedFilter accepts only Soroban transactions whose host function
// trapped or panicked.
//
// "Trapped" is the protocol's word for a contract execution that aborted:
// the WASM host caught the failure and reported it as a result code rather
// than crashing the network. It is distinct from other failure modes such as
// a malformed invocation or an exceeded resource limit, and distinct from a
// transaction that failed for a non-Soroban reason.
//
// The filter reads the decoded Failure on a classified transaction, so it
// must be applied through TransactionFilterSource (or another path that has
// called EnrichTransaction).
func TrappedFilter() TransactionFilter {
	return func(tx *Transaction) bool {
		return tx != nil && tx.Failure.Trapped()
	}
}

// TrapQuery describes a stream of trapped Soroban transactions.
type TrapQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// ErrorResolver optionally maps numeric Soroban contract error codes to
	// enum variant names from the contract spec.
	ErrorResolver ContractErrorResolver
}

// TrapSource is the trapped-contract-execution view of a transaction stream.
type TrapSource = TransactionFilterSource

// NewTrapSource returns a Source emitting only Soroban transactions that
// trapped, with the failing operation and decoded reason attached.
func NewTrapSource(client rpc.Client, q TrapQuery) (*TrapSource, error) {
	src, err := NewTransactionFilterSource(client, TransactionFilterQuery{
		StartLedger:   q.StartLedger,
		PageLimit:     q.PageLimit,
		Filters:       []TransactionFilter{TrappedFilter()},
		ErrorResolver: q.ErrorResolver,
	})
	if err != nil {
		return nil, fmt.Errorf("trap source: %w", err)
	}
	return src, nil
}

// IsTrapped reports whether a transaction record contains a trapped Soroban
// host function. It is the predicate NewTrapSource uses, exported for callers
// that already hold a classified record.
func IsTrapped(tx *Transaction) bool {
	return tx != nil && tx.Failure.Trapped()
}

// ResolveTrappedError extracts a Soroban contract error code from a failed
// transaction (diagnostic events in ResultMetaXDR or textual Error(Contract, #N)
// markers) and applies any resolved enum variant name from resolver.
func ResolveTrappedError(ctx context.Context, tx *Transaction, resolver ContractErrorResolver) {
	if tx == nil || tx.Failure == nil || tx.Failure.Operation == nil {
		return
	}
	if !tx.Failure.Trapped() && !hasContractErrorText(tx) {
		return
	}

	contractID, code, ok := ExtractContractTrapError(tx)
	if !ok {
		return
	}

	display := fmt.Sprintf("Error(Contract, #%d)", code)
	if resolver != nil && contractID != "" {
		if resolved, found := resolver.ResolveContractError(ctx, contractID, code); found && resolved != "" {
			display = resolved
		}
	}

	codeCopy := code
	op := tx.Failure.Operation
	op.ContractID = contractID
	op.ContractErrorCode = &codeCopy
	op.ContractError = display

	replaceContractErrorText(tx, code, display)
}

// ExtractContractTrapError inspects a transaction's ResultMetaXDR diagnostic
// events (and falls back to textual Error(Contract, #N) markers) to return the
// failing contract strkey and numeric error code.
func ExtractContractTrapError(tx *Transaction) (contractID string, code uint32, ok bool) {
	if tx == nil {
		return "", 0, false
	}

	if tx.ResultMetaXDR != "" {
		var meta xdr.TransactionMeta
		if err := xdr.SafeUnmarshalBase64(tx.ResultMetaXDR, &meta); err == nil {
			if cid, errCode, found := diagnosticContractError(&meta); found {
				if cid == "" {
					cid = invokedContractAddress(tx.EnvelopeXDR, tx.Failure)
				}
				return cid, errCode, true
			}
		}
	}

	for _, text := range candidateErrorTexts(tx) {
		if errCode, found := parseContractErrorText(text); found {
			return invokedContractAddress(tx.EnvelopeXDR, tx.Failure), errCode, true
		}
	}
	return "", 0, false
}

func diagnosticContractError(meta *xdr.TransactionMeta) (string, uint32, bool) {
	var diagnostics []xdr.DiagnosticEvent
	switch {
	case meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil:
		diagnostics = meta.V3.SorobanMeta.DiagnosticEvents
	case meta.V == 4 && meta.V4 != nil:
		diagnostics = meta.V4.DiagnosticEvents
	default:
		return "", 0, false
	}

	for _, diag := range diagnostics {
		if diag.Event.Body.V != 0 || diag.Event.Body.V0 == nil {
			continue
		}
		code, ok := contractErrorFromEvent(diag.Event.Body.V0)
		if !ok {
			continue
		}
		var contractID string
		if diag.Event.ContractId != nil {
			if encoded, err := strkey.Encode(strkey.VersionByteContract, diag.Event.ContractId[:]); err == nil {
				contractID = encoded
			}
		}
		return contractID, code, true
	}
	return "", 0, false
}

func contractErrorFromEvent(body *xdr.ContractEventV0) (uint32, bool) {
	for _, topic := range body.Topics {
		if code, ok := scValContractError(topic); ok {
			return code, true
		}
	}
	return scValContractError(body.Data)
}

func scValContractError(val xdr.ScVal) (uint32, bool) {
	if val.Type != xdr.ScValTypeScvError || val.Error == nil {
		return 0, false
	}
	if val.Error.Type != xdr.ScErrorTypeSceContract || val.Error.ContractCode == nil {
		return 0, false
	}
	return uint32(*val.Error.ContractCode), true
}

func invokedContractAddress(envelopeXDR string, failure *Failure) string {
	if envelopeXDR == "" {
		return ""
	}
	var envelope xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(envelopeXDR, &envelope); err != nil {
		return ""
	}
	ops, err := envelopeOperations(&envelope)
	if err != nil {
		return ""
	}

	opIndex := 0
	if failure != nil && failure.Operation != nil && failure.Operation.Index >= 0 && failure.Operation.Index < len(ops) {
		opIndex = failure.Operation.Index
	}
	if opIndex < len(ops) {
		if cid := hostFunctionContractID(&ops[opIndex]); cid != "" {
			return cid
		}
	}
	for i := range ops {
		if cid := hostFunctionContractID(&ops[i]); cid != "" {
			return cid
		}
	}
	return ""
}

func hostFunctionContractID(op *xdr.Operation) string {
	if op.Body.Type != xdr.OperationTypeInvokeHostFunction || op.Body.InvokeHostFunctionOp == nil {
		return ""
	}
	fn := op.Body.InvokeHostFunctionOp.HostFunction
	if fn.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract || fn.InvokeContract == nil {
		return ""
	}
	contract, ok := fn.InvokeContract.ContractAddress.GetContractId()
	if !ok {
		return ""
	}
	encoded, err := strkey.Encode(strkey.VersionByteContract, contract[:])
	if err != nil {
		return ""
	}
	return encoded
}

func candidateErrorTexts(tx *Transaction) []string {
	var texts []string
	if tx.FailureReason != "" {
		texts = append(texts, tx.FailureReason)
	}
	if tx.Failure != nil {
		if tx.Failure.Headline != "" {
			texts = append(texts, tx.Failure.Headline)
		}
		if tx.Failure.Operation != nil {
			if tx.Failure.Operation.Detail.Summary != "" {
				texts = append(texts, tx.Failure.Operation.Detail.Summary)
			}
			if tx.Failure.Operation.Detail.Hint != "" {
				texts = append(texts, tx.Failure.Operation.Detail.Hint)
			}
		}
	}
	return texts
}

func hasContractErrorText(tx *Transaction) bool {
	for _, text := range candidateErrorTexts(tx) {
		if contractErrorTextPattern.MatchString(text) {
			return true
		}
	}
	return false
}

func parseContractErrorText(text string) (uint32, bool) {
	matches := contractErrorTextPattern.FindStringSubmatch(text)
	if len(matches) != 2 {
		return 0, false
	}
	parsed, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(parsed), true
}

func replaceContractErrorText(tx *Transaction, code uint32, replacement string) {
	replaceForCode := func(s string) string {
		if s == "" {
			return s
		}
		return contractErrorTextPattern.ReplaceAllStringFunc(s, func(match string) string {
			parsed, ok := parseContractErrorText(match)
			if !ok || parsed != code {
				return match
			}
			return replacement
		})
	}

	tx.FailureReason = replaceForCode(tx.FailureReason)
	if tx.Failure == nil {
		return
	}
	tx.Failure.Headline = replaceForCode(tx.Failure.Headline)
	if tx.Failure.Operation != nil {
		tx.Failure.Operation.Detail.Summary = replaceForCode(tx.Failure.Operation.Detail.Summary)
		tx.Failure.Operation.Detail.Hint = replaceForCode(tx.Failure.Operation.Detail.Hint)
	}
}
