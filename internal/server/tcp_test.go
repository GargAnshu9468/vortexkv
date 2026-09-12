package server

import (
	"net"
	"strings"
	"testing"

	"github.com/vortexkv/vortexkv/internal/engine"
	"github.com/vortexkv/vortexkv/internal/resp"
)

func TestTCPServerAuthEnforcementOnSubscribe(t *testing.T) {
	eng, err := engine.NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Password = "TestSafePass2026"

	// Find free local port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	srv := NewTCPServer(addr, eng)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// 1. Connect unauthenticated client and attempt SUBSCRIBE
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	w := resp.NewWriter(conn)
	r := resp.NewReader(conn)

	// Send SUBSCRIBE without AUTH
	_ = w.WriteArrayHeader(2)
	_ = w.WriteBulkString("SUBSCRIBE")
	_ = w.WriteBulkString("secret_channel")
	_ = w.Flush()

	val, err := r.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	if val.Type != resp.ErrorPrefix || !strings.Contains(val.Str, "NOAUTH") {
		t.Fatalf("Expected NOAUTH on unauthenticated SUBSCRIBE, got: %v", val)
	}

	// 2. Authenticate
	_ = w.WriteArrayHeader(2)
	_ = w.WriteBulkString("AUTH")
	_ = w.WriteBulkString("TestSafePass2026")
	_ = w.Flush()

	authVal, err := r.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}
	if authVal.Str != "OK" {
		t.Fatalf("Expected OK from AUTH, got: %v", authVal)
	}

	// 3. Send SUBSCRIBE again after AUTH -> should succeed
	_ = w.WriteArrayHeader(2)
	_ = w.WriteBulkString("SUBSCRIBE")
	_ = w.WriteBulkString("secret_channel")
	_ = w.Flush()

	subVal, err := r.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read sub response: %v", err)
	}
	if subVal.Type != resp.ArrayPrefix || len(subVal.Array) < 3 || subVal.Array[0].String() != "subscribe" {
		t.Fatalf("Expected subscribe array response, got: %v", subVal)
	}
}
