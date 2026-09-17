package reactor

import (
	"testing"

	"github.com/vortexkv/vortexkv/internal/resp"
)

func BenchmarkParseCommand_RESPArray(b *testing.B) {
	cmd := []byte("*3\r\n$3\r\nSET\r\n$8\r\nuser:101\r\n$12\r\ncyber_vortex\r\n")
	dst := make([]string, 0, 8)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = ParseCommandInto(cmd, dst)
	}
}

func BenchmarkAppendValue_BulkString(b *testing.B) {
	val := resp.BulkString("cyber_vortex_ultra_fast_record_value")
	buf := make([]byte, 0, 128)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = AppendValue(buf[:0], val)
	}
}

func BenchmarkAppendValue_SimpleString(b *testing.B) {
	val := resp.SimpleString("OK")
	buf := make([]byte, 0, 128)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = AppendValue(buf[:0], val)
	}
}

func BenchmarkRingBuffer_WriteRead(b *testing.B) {
	rb := NewRingBuffer(64 * 1024)
	payload := []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n")
	readBuf := make([]byte, len(payload))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = rb.Write(payload)
		_, _ = rb.Read(readBuf)
	}
}
