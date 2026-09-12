package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vortexkv/vortexkv/internal/engine"
)

func TestReplicationEndpoints(t *testing.T) {
	eng, err := engine.NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	srv := NewServer("127.0.0.1:0", eng)

	// 1. GET /api/replication
	req := httptest.NewRequest(http.MethodGet, "/api/replication", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/replication, got %d: %s", w.Code, w.Body.String())
	}

	var status map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}
	if status["role"] != "master" {
		t.Fatalf("Expected role master, got %v", status["role"])
	}

	// 2. POST /api/replication/promote
	reqPromote := httptest.NewRequest(http.MethodPost, "/api/replication/promote", nil)
	wPromote := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wPromote, reqPromote)

	if wPromote.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/replication/promote, got %d", wPromote.Code)
	}

	// 3. POST /api/replication/replicate
	payload := map[string]any{
		"host": "127.0.0.1",
		"port": 7379,
	}
	b, _ := json.Marshal(payload)
	reqRepl := httptest.NewRequest(http.MethodPost, "/api/replication/replicate", bytes.NewReader(b))
	wRepl := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wRepl, reqRepl)

	if wRepl.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/replication/replicate, got %d", wRepl.Code)
	}
}
