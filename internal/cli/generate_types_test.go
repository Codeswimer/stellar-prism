package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func complexContractSpec() []xdr.ScSpecEntry {
	return []xdr.ScSpecEntry{
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtStructV0,
			UdtStructV0: &xdr.ScSpecUdtStructV0{
				Doc:  "Account balance snapshot.",
				Name: "BalanceSnapshot",
				Fields: []xdr.ScSpecUdtStructFieldV0{
					{Doc: "Owner address.", Name: "owner", Type: addressType()},
					{Name: "amount", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU64}},
					{Name: "signed_delta", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeI64}},
					{Name: "total_supply", Type: i128Type()},
					{Name: "tag", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeSymbol}},
					{Name: "active", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBool}},
					{Name: "digest", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBytesN, BytesN: &xdr.ScSpecTypeBytesN{N: 32}}},
					{
						Name: "memo",
						Type: xdr.ScSpecTypeDef{
							Type:   xdr.ScSpecTypeScSpecTypeOption,
							Option: &xdr.ScSpecTypeOption{ValueType: stringType()},
						},
					},
					{
						Name: "allowances",
						Type: xdr.ScSpecTypeDef{
							Type: xdr.ScSpecTypeScSpecTypeMap,
							Map:  &xdr.ScSpecTypeMap{KeyType: addressType(), ValueType: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU128}},
						},
					},
					{
						Name: "non_string_map",
						Type: xdr.ScSpecTypeDef{
							Type: xdr.ScSpecTypeScSpecTypeMap,
							Map:  &xdr.ScSpecTypeMap{KeyType: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU64}, ValueType: stringType()},
						},
					},
					{
						Name: "items",
						Type: xdr.ScSpecTypeDef{
							Type: xdr.ScSpecTypeScSpecTypeVec,
							Vec:  &xdr.ScSpecTypeVec{ElementType: u32Type()},
						},
					},
					{
						Name: "pair",
						Type: xdr.ScSpecTypeDef{
							Type:  xdr.ScSpecTypeScSpecTypeTuple,
							Tuple: &xdr.ScSpecTypeTuple{ValueTypes: []xdr.ScSpecTypeDef{u32Type(), addressType()}},
						},
					},
					{
						Name: "outcome",
						Type: xdr.ScSpecTypeDef{
							Type: xdr.ScSpecTypeScSpecTypeResult,
							Result: &xdr.ScSpecTypeResult{
								OkType:    xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeVoid},
								ErrorType: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeUdt, Udt: &xdr.ScSpecTypeUdt{Name: "ContractError"}},
							},
						},
					},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtStructV0,
			UdtStructV0: &xdr.ScSpecUdtStructV0{
				Name: "TuplePoint",
				Fields: []xdr.ScSpecUdtStructFieldV0{
					{Name: "0", Type: u32Type()},
					{Name: "1", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU64}},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtUnionV0,
			UdtUnionV0: &xdr.ScSpecUdtUnionV0{
				Name: "Action",
				Cases: []xdr.ScSpecUdtUnionCaseV0{
					{
						Kind:     xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0,
						VoidCase: &xdr.ScSpecUdtUnionCaseVoidV0{Name: "Pause"},
					},
					{
						Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0,
						TupleCase: &xdr.ScSpecUdtUnionCaseTupleV0{
							Name: "Mint",
							Type: []xdr.ScSpecTypeDef{
								addressType(),
								{Type: xdr.ScSpecTypeScSpecTypeU64},
							},
						},
					},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtEnumV0,
			UdtEnumV0: &xdr.ScSpecUdtEnumV0{
				Name: "AssetKind",
				Cases: []xdr.ScSpecUdtEnumCaseV0{
					{Name: "Native", Value: 0},
					{Name: "Issued", Value: 1},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0,
			UdtErrorEnumV0: &xdr.ScSpecUdtErrorEnumV0{
				Name: "ContractError",
				Cases: []xdr.ScSpecUdtErrorEnumCaseV0{
					{Name: "NotAuthorized", Value: 1},
					{Name: "InsufficientBalance", Value: 2},
				},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryEventV0,
			EventV0: &xdr.ScSpecEventV0{
				Doc:  "Emitted when tokens move /* safely */.",
				Name: "Transfer",
				Params: []xdr.ScSpecEventParamV0{
					{Name: "from", Type: addressType(), Location: xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList},
					{Name: "to", Type: addressType(), Location: xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList},
					{Name: "amount", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeU64}, Location: xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationData},
					{Name: "snapshot", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeUdt, Udt: &xdr.ScSpecTypeUdt{Name: "BalanceSnapshot"}}, Location: xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationData},
				},
				DataFormat: xdr.ScSpecEventDataFormatScSpecEventDataFormatSingleValue,
			},
		},
	}
}

func TestGenerateTypeScriptComplexSpec(t *testing.T) {
	t.Parallel()

	ts, err := generateTypeScript(complexContractSpec())
	if err != nil {
		t.Fatalf("generateTypeScript() error = %v", err)
	}

	for _, want := range []string{
		"export interface BalanceSnapshot {",
		"owner: string;",
		"amount: bigint;",
		"signed_delta: bigint;",
		"total_supply: bigint;",
		"tag: string;",
		"active: boolean;",
		"digest: Uint8Array;",
		"memo: string | null;",
		"allowances: Record<string, bigint>;",
		"non_string_map: Array<[bigint, string]>;",
		"items: Array<number>;",
		"pair: [number, string];",
		"outcome: { ok: void } | { err: ContractError };",
		"export type TuplePoint = [number, bigint];",
		"export type Action =",
		`{ tag: "Pause" }`,
		`{ tag: "Mint"; values: [string, bigint] };`,
		"export enum AssetKind {",
		"Native = 0,",
		"Issued = 1,",
		"export enum ContractError {",
		"NotAuthorized = 1,",
		"InsufficientBalance = 2,",
		"export interface Transfer {",
		"/** topic */",
		"/** data */",
		"export type ContractEvent = Transfer;",
	} {
		if !strings.Contains(ts, want) {
			t.Errorf("generated TypeScript missing %q\n%s", want, ts)
		}
	}

	if tscPath, lookErr := exec.LookPath("tsc"); lookErr == nil {
		tsFile := filepath.Join(t.TempDir(), "contract.ts")
		if err := os.WriteFile(tsFile, []byte(ts), 0o600); err != nil {
			t.Fatalf("writing temp ts file: %v", err)
		}
		out, err := exec.Command(tscPath, "--noEmit", "--strict", tsFile).CombinedOutput()
		if err != nil {
			t.Fatalf("tsc --noEmit --strict failed: %v\n%s", err, string(out))
		}
	}
}

func TestGenerateTypeScriptEmptySpecAndDisambiguation(t *testing.T) {
	t.Parallel()

	emptyTS, err := generateTypeScript(nil)
	if err != nil {
		t.Fatalf("generateTypeScript(nil) error = %v", err)
	}
	if !strings.Contains(emptyTS, "export {};") {
		t.Errorf("empty spec output = %q, want export {}", emptyTS)
	}

	entries := []xdr.ScSpecEntry{
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtStructV0,
			UdtStructV0: &xdr.ScSpecUdtStructV0{
				Name:   "Transfer",
				Fields: []xdr.ScSpecUdtStructFieldV0{{Name: "amount", Type: u32Type()}},
			},
		},
		{
			Kind: xdr.ScSpecEntryKindScSpecEntryEventV0,
			EventV0: &xdr.ScSpecEventV0{
				Name:   "Transfer",
				Params: []xdr.ScSpecEventParamV0{{Name: "amount", Type: u32Type()}},
			},
		},
	}
	ts, err := generateTypeScript(entries)
	if err != nil {
		t.Fatalf("generateTypeScript() collision error = %v", err)
	}
	if !strings.Contains(ts, "export interface Transfer {") || !strings.Contains(ts, "export interface TransferEvent {") {
		t.Errorf("expected disambiguated event interface TransferEvent\n%s", ts)
	}
}

func TestGenerateTypesCommandPositionalAndOutFlag(t *testing.T) {
	contractID := testContractID(t)
	srv := fakerpc.NewServer(nil)
	if err := srv.SetContractWasm(contractID, testWasm(t, complexContractSpec()...)); err != nil {
		t.Fatalf("SetContractWasm() error = %v", err)
	}
	withFake(t, srv)

	stdout, _, err := run(t, "generate-types", contractID)
	if err != nil {
		t.Fatalf("Execute() positional error = %v", err)
	}
	if !strings.Contains(stdout, "export interface Transfer {") {
		t.Errorf("stdout missing Transfer interface\n%s", stdout)
	}

	outPath := filepath.Join(t.TempDir(), "generated", "types.ts")
	statusOut, _, err := run(t, "generate-types", "--contract", contractID, "--out", outPath)
	if err != nil {
		t.Fatalf("Execute() --out error = %v", err)
	}
	if !strings.Contains(statusOut, outPath) {
		t.Errorf("status output = %q, want path %q", statusOut, outPath)
	}
	written, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading generated file: %v", err)
	}
	if !strings.Contains(string(written), "export enum ContractError {") {
		t.Errorf("written file missing ContractError enum\n%s", string(written))
	}
}

func TestGenerateTypesCommandValidation(t *testing.T) {
	withFake(t, fakerpc.NewServer(nil))
	contractID := testContractID(t)

	tests := []struct {
		name string
		args []string
	}{
		{name: "missing contract id", args: []string{"generate-types"}},
		{name: "conflicting ids", args: []string{"generate-types", contractID, "--contract", "COTHER"}},
		{name: "invalid contract id", args: []string{"generate-types", "not-a-contract"}},
		{name: "contract not found", args: []string{"generate-types", contractID}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a failure")
			}
		})
	}
}
