package analysis

import "testing"

// TestByteOnlyOperationHasNoLatency: a metered codec reporting wire bytes per
// RPC records a count and no duration. A count is not evidence of a
// distribution, and 0.000ms would be a measured-looking zero for something
// that was never timed — the failure mode the (Value, bool) contract exists to
// prevent.
func TestByteOnlyOperationHasNoLatency(t *testing.T) {
	sf := SummaryFile{Phases: []PhaseEntry{{Name: "", Seconds: 10}}}
	e := OperationEntry{Role: "rpc_server", Operation: "ReadBox", Count: 100,
		TotalRequestBytes: 4000, TotalResponseBytes: 8000}
	c := operationCell(sf, e, "s0", nil)
	v, ok := Latency.Reduce(c)
	if ok && v.HasDist {
		mean, p50, _, p99, max := v.Cells()
		t.Errorf("byte-only series reported a latency distribution: mean=%s p50=%s p99=%s max=%s", mean, p50, p99, max)
	}
}

// ...but its byte rate is still reportable, since that needs only a count, a
// byte total and a window.
func TestByteOnlyOperationStillHasByteRate(t *testing.T) {
	sf := SummaryFile{Phases: []PhaseEntry{{Name: "", Seconds: 10}}}
	e := OperationEntry{Role: "rpc_server", Operation: "ReadBox", Count: 100,
		TotalRequestBytes: 4000, TotalResponseBytes: 8000}
	c := operationCell(sf, e, "s0", nil)

	v, ok := ByteRate.Reduce(c)
	if !ok {
		t.Fatal("byte rate should be computable from a count, bytes and a window")
	}
	if mean, _, _, _, _ := v.Cells(); mean != "1200" {
		t.Errorf("byte rate = %s B/s, want 1200 (12000 bytes over 10s)", mean)
	}
}
