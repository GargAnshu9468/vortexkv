package telemetry

import (
	"bytes"
	"fmt"
	"sort"
)

// PrometheusMetrics generates a standard Prometheus 0.0.4 text exposition
// of VortexKV's live telemetry, compatible with Prometheus scrapers, OpenTelemetry,
// Datadog, and cloud monitoring agents.
func (t *Telemetry) PrometheusMetrics(totalKeys int64, replRole string, connectedReplicas int, replOffset int64) []byte {
	snap := t.GetSnapshot(totalKeys)

	var b bytes.Buffer

	// Up gauge
	b.WriteString("# HELP vortex_up Whether the VortexKV server is operational\n")
	b.WriteString("# TYPE vortex_up gauge\n")
	b.WriteString("vortex_up 1\n\n")

	// Uptime
	b.WriteString("# HELP vortex_uptime_seconds Total uptime of the VortexKV engine in seconds\n")
	b.WriteString("# TYPE vortex_uptime_seconds counter\n")
	fmt.Fprintf(&b, "vortex_uptime_seconds %d\n\n", snap.UptimeSeconds)

	// Replication metrics
	b.WriteString("# HELP vortex_replication_role Cluster role (1 for master, 0 for replica)\n")
	b.WriteString("# TYPE vortex_replication_role gauge\n")
	roleVal := 1
	if replRole == "slave" || replRole == "replica" {
		roleVal = 0
	}
	fmt.Fprintf(&b, "vortex_replication_role %d\n\n", roleVal)

	b.WriteString("# HELP vortex_connected_replicas Number of connected replica nodes\n")
	b.WriteString("# TYPE vortex_connected_replicas gauge\n")
	fmt.Fprintf(&b, "vortex_connected_replicas %d\n\n", connectedReplicas)

	b.WriteString("# HELP vortex_master_repl_offset Current replication offset in bytes\n")
	b.WriteString("# TYPE vortex_master_repl_offset counter\n")
	fmt.Fprintf(&b, "vortex_master_repl_offset %d\n\n", replOffset)

	// Connected clients
	b.WriteString("# HELP vortex_connected_clients Current number of active client connections\n")
	b.WriteString("# TYPE vortex_connected_clients gauge\n")
	fmt.Fprintf(&b, "vortex_connected_clients %d\n\n", snap.ActiveConnections)

	// Total connections accepted
	b.WriteString("# HELP vortex_total_connections_received_total Total client connections accepted\n")
	b.WriteString("# TYPE vortex_total_connections_received_total counter\n")
	fmt.Fprintf(&b, "vortex_total_connections_received_total %d\n\n", snap.TotalConnections)

	// Commands processed total
	b.WriteString("# HELP vortex_commands_processed_total Total commands processed by the engine\n")
	b.WriteString("# TYPE vortex_commands_processed_total counter\n")
	fmt.Fprintf(&b, "vortex_commands_processed_total %d\n\n", snap.TotalCommands)

	// Instantaneous ops per second
	b.WriteString("# HELP vortex_instantaneous_ops_per_sec Current commands processed per second\n")
	b.WriteString("# TYPE vortex_instantaneous_ops_per_sec gauge\n")
	fmt.Fprintf(&b, "vortex_instantaneous_ops_per_sec %d\n\n", snap.OpsPerSecond)

	// Memory allocated bytes
	b.WriteString("# HELP vortex_memory_allocated_bytes Current bytes allocated by the in-memory engine\n")
	b.WriteString("# TYPE vortex_memory_allocated_bytes gauge\n")
	fmt.Fprintf(&b, "vortex_memory_allocated_bytes %d\n\n", snap.AllocatedBytes)

	// Keys total
	b.WriteString("# HELP vortex_keys_total Total number of active keys stored in the keyspace\n")
	b.WriteString("# TYPE vortex_keys_total gauge\n")
	fmt.Fprintf(&b, "vortex_keys_total %d\n\n", snap.TotalKeys)

	// Latency metrics
	b.WriteString("# HELP vortex_command_duration_microseconds_avg Average command execution latency in microseconds\n")
	b.WriteString("# TYPE vortex_command_duration_microseconds_avg gauge\n")
	fmt.Fprintf(&b, "vortex_command_duration_microseconds_avg %.2f\n\n", snap.AvgLatencyMicro)

	b.WriteString("# HELP vortex_command_duration_microseconds_p99 99th percentile command execution latency in microseconds\n")
	b.WriteString("# TYPE vortex_command_duration_microseconds_p99 gauge\n")
	fmt.Fprintf(&b, "vortex_command_duration_microseconds_p99 %.2f\n\n", snap.P99LatencyMicro)

	// Commands by type breakdown
	if len(snap.CommandsByType) > 0 {
		b.WriteString("# HELP vortex_commands_by_type_total Total command executions partitioned by command name\n")
		b.WriteString("# TYPE vortex_commands_by_type_total counter\n")

		// Sort commands deterministically
		cmds := make([]string, 0, len(snap.CommandsByType))
		for cmd := range snap.CommandsByType {
			cmds = append(cmds, cmd)
		}
		sort.Strings(cmds)

		for _, cmd := range cmds {
			fmt.Fprintf(&b, "vortex_commands_by_type_total{command=\"%s\"} %d\n", cmd, snap.CommandsByType[cmd])
		}
		b.WriteString("\n")
	}

	return b.Bytes()
}
