package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/datastruct"
	"github.com/GargAnshu9468/vortexkv/internal/engine"
	"github.com/GargAnshu9468/vortexkv/internal/resp"
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

func TestStreamsEndpoints(t *testing.T) {
	eng, err := engine.NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	srv := NewServer("127.0.0.1:0", eng)

	// 1. POST /api/stream/xadd - Publish first event
	addBody1, _ := json.Marshal(map[string]any{
		"key": "stream:orders",
		"id":  "*",
		"fields": map[string]string{
			"order_id": "ord_1001",
			"customer": "Alice",
			"total":    "99.50",
		},
	})
	reqAdd1 := httptest.NewRequest(http.MethodPost, "/api/stream/xadd", bytes.NewReader(addBody1))
	wAdd1 := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wAdd1, reqAdd1)

	if wAdd1.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/stream/xadd, got %d: %s", wAdd1.Code, wAdd1.Body.String())
	}
	var resAdd1 map[string]any
	_ = json.Unmarshal(wAdd1.Body.Bytes(), &resAdd1)
	msg1ID, ok := resAdd1["id"].(string)
	if !ok || msg1ID == "" {
		t.Fatalf("Expected valid message ID from XADD, got %v", resAdd1["id"])
	}

	// Publish second event
	addBody2, _ := json.Marshal(map[string]any{
		"key": "stream:orders",
		"id":  "*",
		"fields": map[string]string{
			"order_id": "ord_1002",
			"customer": "Bob",
			"total":    "149.00",
		},
	})
	reqAdd2 := httptest.NewRequest(http.MethodPost, "/api/stream/xadd", bytes.NewReader(addBody2))
	wAdd2 := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wAdd2, reqAdd2)
	if wAdd2.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from second /api/stream/xadd, got %d", wAdd2.Code)
	}

	// 2. GET /api/streams - List streams
	reqList := httptest.NewRequest(http.MethodGet, "/api/streams", nil)
	wList := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wList, reqList)

	if wList.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/streams, got %d: %s", wList.Code, wList.Body.String())
	}
	var streamsList struct {
		Streams []engine.StreamMeta `json:"streams"`
		Total   int                 `json:"total"`
	}
	if err := json.Unmarshal(wList.Body.Bytes(), &streamsList); err != nil {
		t.Fatalf("Failed to decode streams list: %v", err)
	}
	if streamsList.Total != 1 || streamsList.Streams[0].Key != "stream:orders" {
		t.Fatalf("Expected stream:orders in list, got %+v", streamsList)
	}
	if streamsList.Streams[0].Length != 2 {
		t.Fatalf("Expected length 2, got %d", streamsList.Streams[0].Length)
	}

	// 3. GET /api/stream/messages - Fetch messages
	reqMsgs := httptest.NewRequest(http.MethodGet, "/api/stream/messages?key=stream:orders", nil)
	wMsgs := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wMsgs, reqMsgs)

	if wMsgs.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/stream/messages, got %d", wMsgs.Code)
	}
	var msgsResp struct {
		Key      string                  `json:"key"`
		Length   int64                   `json:"length"`
		FirstID  string                  `json:"first_id"`
		LastID   string                  `json:"last_id"`
		Messages []datastruct.StreamEntry `json:"messages"`
	}
	if err := json.Unmarshal(wMsgs.Body.Bytes(), &msgsResp); err != nil {
		t.Fatalf("Failed to decode stream messages: %v", err)
	}
	if len(msgsResp.Messages) != 2 {
		t.Fatalf("Expected 2 messages, got %d", len(msgsResp.Messages))
	}
	if msgsResp.Messages[0].Fields["customer"] != "Alice" {
		t.Fatalf("Expected customer Alice, got %v", msgsResp.Messages[0].Fields["customer"])
	}

	// 4. POST /api/stream/group/create - Create consumer group
	createGroupBody, _ := json.Marshal(map[string]any{
		"key":   "stream:orders",
		"group": "fulfillment-workers",
		"id":    "0",
	})
	reqGroupCreate := httptest.NewRequest(http.MethodPost, "/api/stream/group/create", bytes.NewReader(createGroupBody))
	wGroupCreate := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wGroupCreate, reqGroupCreate)

	if wGroupCreate.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/stream/group/create, got %d: %s", wGroupCreate.Code, wGroupCreate.Body.String())
	}

	// Read message into consumer group PEL via engine command
	readRes := eng.ExecuteCommand("test-worker", []string{
		"XREADGROUP", "GROUP", "fulfillment-workers", "worker-alpha", "COUNT", "1", "STREAMS", "stream:orders", ">",
	})
	if readRes.Type == resp.ErrorPrefix {
		t.Fatalf("XREADGROUP failed: %s", readRes.String())
	}

	// 5. GET /api/stream/groups - Inspect groups and PEL
	reqGroups := httptest.NewRequest(http.MethodGet, "/api/stream/groups?key=stream:orders", nil)
	wGroups := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wGroups, reqGroups)

	if wGroups.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/stream/groups, got %d", wGroups.Code)
	}
	var groupsResp struct {
		Key    string        `json:"key"`
		Groups []GroupDetail `json:"groups"`
	}
	if err := json.Unmarshal(wGroups.Body.Bytes(), &groupsResp); err != nil {
		t.Fatalf("Failed to decode stream groups: %v", err)
	}
	if len(groupsResp.Groups) != 1 {
		t.Fatalf("Expected 1 consumer group, got %d", len(groupsResp.Groups))
	}
	group := groupsResp.Groups[0]
	if group.Name != "fulfillment-workers" {
		t.Fatalf("Expected fulfillment-workers group, got %s", group.Name)
	}
	if group.PendingCount != 1 {
		t.Fatalf("Expected 1 pending message, got %d", group.PendingCount)
	}
	if len(group.PEL) != 1 || group.PEL[0].Consumer != "worker-alpha" {
		t.Fatalf("Expected 1 PEL item assigned to worker-alpha, got %+v", group.PEL)
	}

	// 6. POST /api/stream/xack - Acknowledge the pending message
	ackBody, _ := json.Marshal(map[string]any{
		"key":   "stream:orders",
		"group": "fulfillment-workers",
		"ids":   []string{msg1ID},
	})
	reqAck := httptest.NewRequest(http.MethodPost, "/api/stream/xack", bytes.NewReader(ackBody))
	wAck := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wAck, reqAck)

	if wAck.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/stream/xack, got %d: %s", wAck.Code, wAck.Body.String())
	}
	var ackResp map[string]any
	_ = json.Unmarshal(wAck.Body.Bytes(), &ackResp)
	if acked, ok := ackResp["acknowledged"].(float64); !ok || int(acked) != 1 {
		t.Fatalf("Expected acknowledged count 1, got %v", ackResp["acknowledged"])
	}

	// Verify PEL is now empty
	wGroupsAfter := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(wGroupsAfter, reqGroups)
	var groupsAfterResp struct {
		Groups []GroupDetail `json:"groups"`
	}
	_ = json.Unmarshal(wGroupsAfter.Body.Bytes(), &groupsAfterResp)
	if groupsAfterResp.Groups[0].PendingCount != 0 {
		t.Fatalf("Expected 0 pending messages after ACK, got %d", groupsAfterResp.Groups[0].PendingCount)
	}
}
