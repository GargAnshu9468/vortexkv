package reactor

import (
	"bytes"
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestAppendValue(t *testing.T) {
	tests := []struct {
		name     string
		val      resp.Value
		expected []byte
	}{
		{"OK", resp.SimpleString("OK"), []byte("+OK\r\n")},
		{"PONG", resp.SimpleString("PONG"), []byte("+PONG\r\n")},
		{"Error", resp.Error("ERR unknown"), []byte("-ERR unknown\r\n")},
		{"Integer", resp.Integer(42), []byte(":42\r\n")},
		{"Zero", resp.Integer(0), []byte(":0\r\n")},
		{"BulkString", resp.BulkString("hello"), []byte("$5\r\nhello\r\n")},
		{"NullBulk", resp.Null(), []byte("$-1\r\n")},
		{"Array", resp.Array([]resp.Value{resp.BulkString("a"), resp.Integer(1)}), []byte("*2\r\n$1\r\na\r\n:1\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 0, 128)
			buf = AppendValue(buf, tt.val)
			if !bytes.Equal(buf, tt.expected) {
				t.Fatalf("expected %q, got %q", tt.expected, buf)
			}
		})
	}
}
