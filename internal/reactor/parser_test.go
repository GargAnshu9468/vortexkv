package reactor

import (
	"reflect"
	"testing"
)

func TestParseCommand_RESPArray(t *testing.T) {
	raw := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
	args, consumed, err := ParseCommand(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if consumed != len(raw) {
		t.Fatalf("expected consumed %d, got %d", len(raw), consumed)
	}

	expected := []string{"SET", "mykey", "myvalue"}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("expected %v, got %v", expected, args)
	}
}

func TestParseCommand_Incomplete(t *testing.T) {
	partial := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmy")
	args, consumed, err := ParseCommand(partial)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args != nil || consumed != 0 {
		t.Fatalf("expected incomplete nil, got args=%v, consumed=%d", args, consumed)
	}
}

func TestParseCommand_Inline(t *testing.T) {
	inline := []byte("PING\r\n")
	args, consumed, err := ParseCommand(inline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if consumed != len(inline) {
		t.Fatalf("expected consumed %d, got %d", len(inline), consumed)
	}
	if len(args) != 1 || args[0] != "PING" {
		t.Fatalf("expected PING, got %v", args)
	}
}

func TestParseCommand_PipelinedBatch(t *testing.T) {
	cmd1 := []byte("*1\r\n$4\r\nPING\r\n")
	cmd2 := []byte("*2\r\n$3\r\nGET\r\n$1\r\na\r\n")
	batch := append(cmd1, cmd2...)

	args1, c1, err1 := ParseCommand(batch)
	if err1 != nil || len(args1) != 1 || args1[0] != "PING" || c1 != len(cmd1) {
		t.Fatalf("cmd1 failed: args=%v, c1=%d, err=%v", args1, c1, err1)
	}

	args2, c2, err2 := ParseCommand(batch[c1:])
	if err2 != nil || len(args2) != 2 || args2[0] != "GET" || args2[1] != "a" || c2 != len(cmd2) {
		t.Fatalf("cmd2 failed: args=%v, c2=%d, err=%v", args2, c2, err2)
	}
}
