package reactor

import (
	"bytes"
	"io"
	"testing"
)

func TestRingBuffer_BasicReadWrite(t *testing.T) {
	rb := NewRingBuffer(16)
	msg := []byte("hello world")

	n, err := rb.Write(msg)
	if err != nil || n != len(msg) {
		t.Fatalf("write failed: n=%d, err=%v", n, err)
	}

	if rb.Len() != len(msg) {
		t.Fatalf("expected len %d, got %d", len(msg), rb.Len())
	}

	buf := make([]byte, len(msg))
	rn, rerr := rb.Read(buf)
	if rerr != nil || rn != len(msg) {
		t.Fatalf("read failed: rn=%d, rerr=%v", rn, rerr)
	}

	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %s, got %s", msg, buf)
	}

	if !rb.IsEmpty() {
		t.Fatalf("expected empty buffer")
	}
}

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := NewRingBuffer(16)

	// Fill partially and read partially to move cursors
	rb.Write([]byte("12345678"))
	tmp := make([]byte, 6)
	rb.Read(tmp) // r=6, w=8

	// Write across edge
	rb.Write([]byte("ABCDEFGHI")) // len 9, wraps around

	expected := []byte("78ABCDEFGHI")
	got := rb.Bytes()
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %s, got %s", expected, got)
	}

	readBack := make([]byte, len(expected))
	n, err := rb.Read(readBack)
	if err != nil && err != io.EOF {
		t.Fatalf("read failed: %v", err)
	}
	if n != len(expected) || !bytes.Equal(readBack, expected) {
		t.Fatalf("expected %s, got %s", expected, readBack)
	}
}

func TestRingBuffer_Grow(t *testing.T) {
	rb := NewRingBuffer(8)
	largeMsg := []byte("this is a very long message that must trigger buffer expansion")

	n, err := rb.Write(largeMsg)
	if err != nil || n != len(largeMsg) {
		t.Fatalf("write failed: %v", err)
	}

	if rb.Len() != len(largeMsg) {
		t.Fatalf("len mismatch: %d != %d", rb.Len(), len(largeMsg))
	}

	out := rb.Bytes()
	if !bytes.Equal(out, largeMsg) {
		t.Fatalf("mismatch after grow")
	}
}
