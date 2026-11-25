package stream_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestTrapSourceEmitsOnlyTrappedExecutions(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(
		hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		classifiedTransaction(40, 1, "ok", envelope, successResult(t), true),
		classifiedTransaction(40, 2, "trap", envelope, trappedResult(t), false),
		classifiedTransaction(40, 3, "limit", envelope, resourceLimitResult(t), false),
	})

	src, err := stream.NewTrapSource(srv, stream.TrapQuery{})
	if err != nil {
		t.Fatalf("NewTrapSource() error = %v", err)
	}
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("got %d records, want only the trapped execution", len(batch.Records))
	}

	record := batch.Records[0]
	if record.Transaction.Hash != "trap" {
		t.Errorf("hash = %q, want the trapped transaction", record.Transaction.Hash)
	}
	if !stream.IsTrapped(record.Transaction) {
		t.Error("IsTrapped() = false, want true")
	}
	if record.Transaction.Failure == nil || record.Transaction.Failure.Operation == nil {
		t.Fatal("record carries no decoded operation failure")
	}
	if record.Transaction.Failure.Operation.Trapped != true {
		t.Error("operation failure was not marked trapped")
	}
}

func TestTrappedFilterRejectsNonTraps(t *testing.T) {
	t.Parallel()

	filter := stream.TrappedFilter()
	if filter(nil) {
		t.Error("TrappedFilter() accepted a nil transaction")
	}
	if filter(&stream.Transaction{}) {
		t.Error("TrappedFilter() accepted an unclassified transaction")
	}
	if filter(&stream.Transaction{Failure: &stream.Failure{}}) {
		t.Error("TrappedFilter() accepted a failure with no operation")
	}
	if filter(&stream.Transaction{Failure: &stream.Failure{
		Operation: &stream.OperationFailure{Trapped: false},
	}}) {
		t.Error("TrappedFilter() accepted a non-trap operation failure")
	}
	if !filter(&stream.Transaction{Failure: &stream.Failure{
		Operation: &stream.OperationFailure{Trapped: true},
	}}) {
		t.Error("TrappedFilter() rejected a trap")
	}
}

type stubContractErrorResolver struct {
	names map[uint32]string
}

func (s stubContractErrorResolver) ResolveContractError(_ context.Context, _ string, code uint32) (string, bool) {
	name, ok := s.names[code]
	return name, ok
}

func TestResolveTrappedErrorFromV3DiagnosticMetaAndTextFallback(t *testing.T) {
	t.Parallel()

	rawID := make([]byte, 32)
	rawID[0] = 0x11
	contractID, err := strkey.Encode(strkey.VersionByteContract, rawID)
	if err != nil {
		t.Fatalf("strkey.Encode() error = %v", err)
	}
	var xdrContractID xdr.ContractId
	copy(xdrContractID[:], rawID)

	errCode := xdr.Uint32(1)
	errSym := xdr.ScSymbol("error")
	metaV3 := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				DiagnosticEvents: []xdr.DiagnosticEvent{{
					Event: xdr.ContractEvent{
						ContractId: &xdrContractID,
						Type:       xdr.ContractEventTypeDiagnostic,
						Body: xdr.ContractEventBody{
							V: 0,
							V0: &xdr.ContractEventV0{
								Topics: []xdr.ScVal{
									{Type: xdr.ScValTypeScvSymbol, Sym: &errSym},
									{
										Type: xdr.ScValTypeScvError,
										Error: &xdr.ScError{
											Type:         xdr.ScErrorTypeSceContract,
											ContractCode: &errCode,
										},
									},
								},
							},
						},
					},
				}},
			},
		},
	}
	metaBytes, err := metaV3.MarshalBinary()
	if err != nil {
		t.Fatalf("metaV3.MarshalBinary() error = %v", err)
	}

	envelope := encodeEnvelope(t, v1Envelope(
		hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	info := classifiedTransaction(42, 1, "trap-v3", envelope, trappedResult(t), false)
	info.ResultMetaXDR = base64.StdEncoding.EncodeToString(metaBytes)

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{info})
	src, err := stream.NewTrapSource(srv, stream.TrapQuery{
		ErrorResolver: stubContractErrorResolver{names: map[uint32]string{1: "ContractError::NotAuthorized"}},
	})
	if err != nil {
		t.Fatalf("NewTrapSource() error = %v", err)
	}
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("got %d records, want 1", len(batch.Records))
	}
	op := batch.Records[0].Transaction.Failure.Operation
	if op.ContractID != contractID {
		t.Errorf("ContractID = %q, want %q", op.ContractID, contractID)
	}
	if op.ContractErrorCode == nil || *op.ContractErrorCode != 1 {
		t.Errorf("ContractErrorCode = %v, want 1", op.ContractErrorCode)
	}
	if op.ContractError != "ContractError::NotAuthorized" {
		t.Errorf("ContractError = %q, want ContractError::NotAuthorized", op.ContractError)
	}

	// Textual fallback replaces raw Error(Contract, #1) when meta is absent.
	fallbackTx := &stream.Transaction{
		EnvelopeXDR: encodeEnvelope(t, v1Envelope(hostFunctionOp(xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &xdrContractID,
				},
				FunctionName: "withdraw",
			},
		}))),
		FailureReason: "HostError: Error(Contract, #1)",
		Failure: &stream.Failure{
			Headline: "HostError: Error(Contract, #1)",
			Operation: &stream.OperationFailure{
				Trapped: true,
				Detail:  stream.FailureDetail{Summary: "Error(Contract, #1)"},
			},
		},
	}
	stream.ResolveTrappedError(context.Background(), fallbackTx, stubContractErrorResolver{
		names: map[uint32]string{1: "ContractError::NotAuthorized"},
	})
	if strings.Contains(fallbackTx.Failure.Operation.Detail.Summary, "Error(Contract, #1)") {
		t.Errorf("summary was not rewritten: %q", fallbackTx.Failure.Operation.Detail.Summary)
	}
	if fallbackTx.Failure.Operation.ContractError != "ContractError::NotAuthorized" {
		t.Errorf("fallback ContractError = %q, want ContractError::NotAuthorized", fallbackTx.Failure.Operation.ContractError)
	}
}
