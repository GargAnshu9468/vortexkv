package resp

import (
	"bytes"
	"testing"
)

func TestRESPParserAndWriter(t *testing.T) {
	// 1. Test Array command parsing: *3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nadmin\r\n
	input := "*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nadmin\r\n"
	r := NewReader(bytes.NewBufferString(input))
	cmd, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("Failed to read command: %v", err)
	}
	if len(cmd) != 3 || cmd[0] != "SET" || cmd[1] != "user" || cmd[2] != "admin" {
		t.Fatalf("Unexpected parsed command: %v", cmd)
	}

	// 2. Test Inline Command parsing: SET "my key" "my value"\r\n
	inlineInput := "SET \"my key\" \"my value\"\r\n"
	rInline := NewReader(bytes.NewBufferString(inlineInput))
	cmdInline, err := rInline.ReadCommand()
	if err != nil {
		t.Fatalf("Failed to read inline command: %v", err)
	}
	if len(cmdInline) != 3 || cmdInline[0] != "SET" || cmdInline[1] != "my key" || cmdInline[2] != "my value" {
		t.Fatalf("Unexpected parsed inline command: %v", cmdInline)
	}

	// 3. Test Writer
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteOK(); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteInteger(42); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulkString("vortex"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNull(); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	expected := "+OK\r\n:42\r\n$6\r\nvortex\r\n$-1\r\n"
	if buf.String() != expected {
		t.Fatalf("Writer output mismatch: got %q, want %q", buf.String(), expected)
	}
}

func TestRESPParserArrayBounds(t *testing.T) {
	// Oversized array header (e.g. 10M elements) should return ErrMultibulkTooLarge immediately without allocating
	oversizedInput := "*10000000\r\n"
	r := NewReader(bytes.NewBufferString(oversizedInput))
	_, err := r.ReadValue()
	if err != ErrMultibulkTooLarge {
		t.Fatalf("Expected ErrMultibulkTooLarge, got: %v", err)
	}
}
