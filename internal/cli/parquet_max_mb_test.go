package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestParquetMaxMBFlagRotatesParts drives --parquet-max-mb through the real
// flag parsing and sink wiring. One megabyte is the smallest value the flag
// accepts, so two 600 KiB records reach the limit and the third starts a new
// part — proof the megabyte value reached ParquetConfig.MaxFileBytes rather
// than being ignored or treated as bytes.
func TestParquetMaxMBFlagRotatesParts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	var sf sinkFlags
	cmd := &cobra.Command{Use: "events"}
	sf.register(cmd)
	if err := cmd.ParseFlags([]string{"--sink-parquet-dir", dir, "--parquet-max-mb", "1"}); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}
	if err := sf.validate(cmd); err != nil {
		t.Fatalf("validate: %v", err)
	}

	out, err := sf.addTo(stream.SinkFunc(func(stream.Record) error { return nil }), false, nil)
	if err != nil {
		t.Fatalf("addTo: %v", err)
	}
	t.Cleanup(func() {
		for _, closer := range sf.constructedSinks {
			_ = closer.Close()
		}
	})

	payload := strings.Repeat("a", 600*1024)
	for i := range 3 {
		r := stream.Record{
			ID:       fmt.Sprintf("%010d-0000", 100+i),
			Sequence: uint32(100 + i),
			Time:     time.Unix(1700000100+int64(i), 0).UTC(),
			Kind:     stream.KindEvent,
			Event: &stream.Event{
				Type:       "contract",
				ContractID: "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
				TxHash:     "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
				TxIndex:    uint32(i),
				OpIndex:    0,
				TopicXDR:   []string{"AAAAAwAAAAc="},
				ValueXDR:   payload,
			},
		}
		if err := out.Emit(r); err != nil {
			t.Fatalf("Emit(%d): %v", i, err)
		}
	}
	if err := out.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatalf("globbing parts: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("wrote %d part files, want 2 (2 rows at 600 KiB with a 1 MB limit, then 1 row)", len(files))
	}
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			t.Fatalf("stat %s: %v", f, err)
		}
		if info.Size() == 0 {
			t.Errorf("part %s is empty", filepath.Base(f))
		}
	}
}

func TestParquetMaxMBRejectsNegative(t *testing.T) {
	t.Parallel()

	var sf sinkFlags
	cmd := &cobra.Command{Use: "events"}
	sf.register(cmd)
	if err := cmd.ParseFlags([]string{"--parquet-max-mb=-1"}); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}
	err := sf.validate(cmd)
	if err == nil || !strings.Contains(err.Error(), "--parquet-max-mb must not be negative") {
		t.Fatalf("validate error = %v, want one rejecting a negative --parquet-max-mb", err)
	}
}
