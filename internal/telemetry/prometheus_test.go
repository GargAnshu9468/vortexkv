package telemetry

import (
	"strings"
	"testing"
)

func TestPrometheusMetricsExposition(t *testing.T) {
	tel := NewTelemetry()
	tel.IncrConnections()
	tel.RecordCommand("GET", 150, []string{"user:1"})
	tel.RecordCommand("SET", 220, []string{"user:1", "val"})

	rawMetrics := string(tel.PrometheusMetrics(42))

	expectedSubstrings := []string{
		"# HELP vortex_up",
		"vortex_up 1",
		"# HELP vortex_uptime_seconds",
		"vortex_connected_clients 1",
		"vortex_keys_total 42",
		"vortex_commands_processed_total 2",
		`vortex_commands_by_type_total{command="GET"} 1`,
		`vortex_commands_by_type_total{command="SET"} 1`,
	}

	for _, s := range expectedSubstrings {
		if !strings.Contains(rawMetrics, s) {
			t.Errorf("Expected Prometheus metrics to contain %q, but got:\n%s", s, rawMetrics)
		}
	}
}
