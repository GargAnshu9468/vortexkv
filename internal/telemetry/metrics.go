package telemetry

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type SlowLogEntry struct {
	ID        int64    `json:"id"`
	Timestamp int64    `json:"timestamp"`
	DurationMicro int64 `json:"duration_micro"`
	Command   []string `json:"command"`
}

type MetricsSnapshot struct {
	UptimeSeconds      int64             `json:"uptime_seconds"`
	TotalConnections   int64             `json:"total_connections"`
	ActiveConnections  int64             `json:"active_connections"`
	TotalCommands      int64             `json:"total_commands"`
	OpsPerSecond       int64             `json:"ops_per_second"`
	AvgLatencyMicro    float64           `json:"avg_latency_micro"`
	P99LatencyMicro    float64           `json:"p99_latency_micro"`
	AllocatedBytes     uint64            `json:"allocated_bytes"`
	TotalKeys          int64             `json:"total_keys"`
	CommandsByType     map[string]int64  `json:"commands_by_type"`
	RecentSlowLogs     []SlowLogEntry    `json:"recent_slow_logs"`
}

type Telemetry struct {
	startTime          time.Time
	totalConnections   atomic.Int64
	activeConnections  atomic.Int64
	totalCommands      atomic.Int64
	lastSecondCommands atomic.Int64
	opsPerSecond       atomic.Int64

	latencySamples     [latencySamplesCount]int64
	latencyIdx         atomic.Uint64

	cmdCounters        sync.Map // map[string]*atomic.Int64

	muSlowLog          sync.RWMutex
	slowLogs           []SlowLogEntry
	slowLogCounter     atomic.Int64
	slowThresholdMicro int64

	// Event streaming listeners (for Web Studio WebSocket)
	muListeners        sync.RWMutex
	listeners          map[chan MetricsSnapshot]struct{}
	eventListeners     map[chan map[string]any]struct{}
	eventListenerCount atomic.Int32
}

const latencySamplesCount = 1024

func NewTelemetry() *Telemetry {
	t := &Telemetry{
		startTime:          time.Now(),
		slowLogs:           make([]SlowLogEntry, 0, 128),
		slowThresholdMicro: 10000, // 10ms default
		listeners:          make(map[chan MetricsSnapshot]struct{}),
		eventListeners:     make(map[chan map[string]any]struct{}),
	}

	go t.rateCalculatorLoop()
	return t
}

func (t *Telemetry) rateCalculatorLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentTotal := t.totalCommands.Load()
		lastTotal := t.lastSecondCommands.Swap(currentTotal)
		diff := currentTotal - lastTotal
		if diff < 0 {
			diff = 0
		}
		t.opsPerSecond.Store(diff)
		t.broadcastSnapshot(t.GetSnapshot(0))
	}
}

func (t *Telemetry) IncrConnections() {
	t.totalConnections.Add(1)
	t.activeConnections.Add(1)
}

func (t *Telemetry) DecrConnections() {
	t.activeConnections.Add(-1)
}

func (t *Telemetry) RecordCommand(cmdName string, durationMicro int64, args []string) {
	total := t.totalCommands.Add(1)

	// Sample latency into lock-free circular ring
	if total&31 == 0 {
		idx := t.latencyIdx.Add(1) % latencySamplesCount
		atomic.StoreInt64(&t.latencySamples[idx], durationMicro)
	}

	// Update per-command counter lock-free
	if val, ok := t.cmdCounters.Load(cmdName); ok {
		val.(*atomic.Int64).Add(1)
	} else {
		ctr := new(atomic.Int64)
		ctr.Store(1)
		actual, loaded := t.cmdCounters.LoadOrStore(cmdName, ctr)
		if loaded {
			actual.(*atomic.Int64).Add(1)
		}
	}

	// SlowLog check (only triggered when command is >= threshold, default 10ms)
	if durationMicro >= t.slowThresholdMicro {
		safeArgs := make([]string, len(args))
		copy(safeArgs, args)
		if cmdName == "AUTH" {
			for i := 1; i < len(safeArgs); i++ {
				safeArgs[i] = "[REDACTED]"
			}
		}

		t.muSlowLog.Lock()
		entry := SlowLogEntry{
			ID:            t.slowLogCounter.Add(1),
			Timestamp:     time.Now().Unix(),
			DurationMicro: durationMicro,
			Command:       safeArgs,
		}
		if len(t.slowLogs) >= 128 {
			t.slowLogs = t.slowLogs[1:]
		}
		t.slowLogs = append(t.slowLogs, entry)
		t.muSlowLog.Unlock()

		t.BroadcastEvent(map[string]any{
			"type": "slowlog",
			"data": entry,
		})
	}
}

func (t *Telemetry) HasEventListeners() bool {
	return t.eventListenerCount.Load() > 0
}

func (t *Telemetry) BroadcastEvent(event map[string]any) {
	if t.eventListenerCount.Load() == 0 {
		return
	}
	t.muListeners.RLock()
	defer t.muListeners.RUnlock()

	for ch := range t.eventListeners {
		select {
		case ch <- event:
		default:
		}
	}
}

func (t *Telemetry) SubscribeEvents() chan map[string]any {
	ch := make(chan map[string]any, 100)
	t.muListeners.Lock()
	t.eventListeners[ch] = struct{}{}
	t.eventListenerCount.Add(1)
	t.muListeners.Unlock()
	return ch
}

func (t *Telemetry) UnsubscribeEvents(ch chan map[string]any) {
	t.muListeners.Lock()
	if _, exists := t.eventListeners[ch]; exists {
		delete(t.eventListeners, ch)
		t.eventListenerCount.Add(-1)
		close(ch)
	}
	t.muListeners.Unlock()
}

func (t *Telemetry) broadcastSnapshot(snap MetricsSnapshot) {
	t.muListeners.RLock()
	defer t.muListeners.RUnlock()

	for ch := range t.listeners {
		select {
		case ch <- snap:
		default:
		}
	}
}

func (t *Telemetry) SubscribeSnapshots() chan MetricsSnapshot {
	ch := make(chan MetricsSnapshot, 10)
	t.muListeners.Lock()
	t.listeners[ch] = struct{}{}
	t.muListeners.Unlock()
	return ch
}

func (t *Telemetry) UnsubscribeSnapshots(ch chan MetricsSnapshot) {
	t.muListeners.Lock()
	delete(t.listeners, ch)
	close(ch)
	t.muListeners.Unlock()
}

func (t *Telemetry) GetSnapshot(totalKeys int64) MetricsSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate latency percentiles from atomic sample ring
	var sorted []int64
	for i := 0; i < latencySamplesCount; i++ {
		v := atomic.LoadInt64(&t.latencySamples[i])
		if v > 0 {
			sorted = append(sorted, v)
		}
	}
	var avgLatency float64
	var p99Latency float64
	if len(sorted) > 0 {
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		var sum int64
		for _, v := range sorted {
			sum += v
		}
		avgLatency = float64(sum) / float64(len(sorted))
		p99Idx := int(float64(len(sorted)-1) * 0.99)
		p99Latency = float64(sorted[p99Idx])
	}

	// Clone command counts lock-free
	cmdCopy := make(map[string]int64)
	t.cmdCounters.Range(func(key, value any) bool {
		cmdCopy[key.(string)] = value.(*atomic.Int64).Load()
		return true
	})

	// Clone recent slow logs
	t.muSlowLog.RLock()
	slowCopy := make([]SlowLogEntry, len(t.slowLogs))
	copy(slowCopy, t.slowLogs)
	t.muSlowLog.RUnlock()

	return MetricsSnapshot{
		UptimeSeconds:     int64(time.Since(t.startTime).Seconds()),
		TotalConnections:  t.totalConnections.Load(),
		ActiveConnections: t.activeConnections.Load(),
		TotalCommands:     t.totalCommands.Load(),
		OpsPerSecond:      t.opsPerSecond.Load(),
		AvgLatencyMicro:   avgLatency,
		P99LatencyMicro:   p99Latency,
		AllocatedBytes:    m.Alloc,
		TotalKeys:         totalKeys,
		CommandsByType:    cmdCopy,
		RecentSlowLogs:    slowCopy,
	}
}

// GenerateRedisInfo generates the standard Redis INFO response text
func (t *Telemetry) GenerateRedisInfo(totalKeys int64) string {
	snap := t.GetSnapshot(totalKeys)
	return fmt.Sprintf(
		"# Server\r\n"+
			"redis_version:7.2.0-vortex-1.0.0\r\n"+
			"vortexkv_version:1.0.0-PROD\r\n"+
			"os:%s\r\n"+
			"arch:%s\r\n"+
			"uptime_in_seconds:%d\r\n"+
			"# Clients\r\n"+
			"connected_clients:%d\r\n"+
			"# Memory\r\n"+
			"used_memory:%d\r\n"+
			"used_memory_human:%.2fM\r\n"+
			"# Stats\r\n"+
			"total_connections_received:%d\r\n"+
			"total_commands_processed:%d\r\n"+
			"instantaneous_ops_per_sec:%d\r\n"+
			"# Keyspace\r\n"+
			"db0:keys=%d,expires=0,avg_ttl=0\r\n",
		runtime.GOOS,
		runtime.GOARCH,
		snap.UptimeSeconds,
		snap.ActiveConnections,
		snap.AllocatedBytes,
		float64(snap.AllocatedBytes)/(1024*1024),
		snap.TotalConnections,
		snap.TotalCommands,
		snap.OpsPerSecond,
		totalKeys,
	)
}
