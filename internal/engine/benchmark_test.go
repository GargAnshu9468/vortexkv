package engine

import (
	"fmt"
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func BenchmarkKeyspace_Get_Parallel(b *testing.B) {
	ks := NewKeyspace()
	defer ks.Close()

	// Seed 10,000 keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		ks.Set(key, &Entry{
			Type:  TypeString,
			Value: "bench_value_payload",
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:key:%d", i%10000)
			entry, ok := ks.Get(key)
			if !ok || entry == nil {
				b.Fatalf("expected key %s to exist", key)
			}
			i++
		}
	})
}

func BenchmarkKeyspace_Set_Parallel(b *testing.B) {
	ks := NewKeyspace()
	defer ks.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:set:%d", i%10000)
			ks.Set(key, &Entry{
				Type:  TypeString,
				Value: "new_payload",
			})
			i++
		}
	})
}

func BenchmarkEngine_Execute_Get_Parallel(b *testing.B) {
	eng, err := NewEngine("", "")
	if err != nil {
		b.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("user:%d", i)
		eng.ExecuteCommand("init", []string{"SET", key, "vortex_hyper_speed"})
	}

	session := &ClientSession{
		Authenticated: true,
		Username:      "default",
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("user:%d", i%10000)
			val := eng.ExecuteCommandWithSession(session, "bench-client", []string{"GET", key})
			if val.Type != resp.BulkStringPrefix {
				b.Fatalf("unexpected response: %v", val)
			}
			i++
		}
	})
}

func BenchmarkEngine_Execute_Set_Parallel(b *testing.B) {
	eng, err := NewEngine("", "")
	if err != nil {
		b.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	session := &ClientSession{
		Authenticated: true,
		Username:      "default",
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("set:%d", i%10000)
			val := eng.ExecuteCommandWithSession(session, "bench-client", []string{"SET", key, "fast_val"})
			if val.Type != resp.SimpleStringPrefix || val.Str != "OK" {
				b.Fatalf("unexpected set response: %v", val)
			}
			i++
		}
	})
}

func BenchmarkEngine_Execute_Ping_Parallel(b *testing.B) {
	eng, err := NewEngine("", "")
	if err != nil {
		b.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	session := &ClientSession{
		Authenticated: true,
		Username:      "default",
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			val := eng.ExecuteCommandWithSession(session, "bench-client", []string{"PING"})
			if val.Type != resp.SimpleStringPrefix || val.Str != "PONG" {
				b.Fatalf("unexpected ping response: %v", val)
			}
		}
	})
}
