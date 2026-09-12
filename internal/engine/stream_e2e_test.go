package engine

import (
	"testing"
	"time"

	"github.com/vortexkv/vortexkv/internal/resp"
)

func TestStreamEngineE2E(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// 1. Test XADD with field order and auto-id
	res := eng.ExecuteCommand("c1", []string{"XADD", "mystream", "*", "sensor", "temperature", "temp", "24.5"})
	if res.Type != resp.BulkStringPrefix {
		t.Fatalf("Expected bulk string ID from XADD, got %v", res)
	}
	id1 := string(res.Bulk)

	res = eng.ExecuteCommand("c1", []string{"XADD", "mystream", "*", "sensor", "humidity", "humidity", "65%"})
	id2 := string(res.Bulk)

	res = eng.ExecuteCommand("c1", []string{"XADD", "mystream", "*", "sensor", "pressure", "hpa", "1013"})
	id3 := string(res.Bulk)

	// 2. Test XLEN
	res = eng.ExecuteCommand("c1", []string{"XLEN", "mystream"})
	if res.Type != resp.IntegerPrefix || res.Num != 3 {
		t.Fatalf("Expected XLEN == 3, got %v", res)
	}

	// 3. Test XRANGE & XREVRANGE
	res = eng.ExecuteCommand("c1", []string{"XRANGE", "mystream", "-", "+", "COUNT", "2"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 2 {
		t.Fatalf("Expected XRANGE to return 2 items, got %v", res)
	}

	res = eng.ExecuteCommand("c1", []string{"XREVRANGE", "mystream", "+", "-", "COUNT", "1"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 1 || string(res.Array[0].Array[0].Bulk) != id3 {
		t.Fatalf("Expected XREVRANGE top item to be %s, got %v", id3, res)
	}

	// 4. Test XREAD non-blocking
	res = eng.ExecuteCommand("c1", []string{"XREAD", "COUNT", "2", "STREAMS", "mystream", id1})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 1 {
		t.Fatalf("Expected XREAD to return stream array, got %v", res)
	}
	streamItems := res.Array[0].Array[1].Array
	if len(streamItems) != 2 {
		t.Fatalf("Expected 2 items from XREAD, got %d", len(streamItems))
	}

	// 5. Test XGROUP CREATE with MKSTREAM on new stream
	res = eng.ExecuteCommand("c1", []string{"XGROUP", "CREATE", "newstream", "worker_grp", "$", "MKSTREAM"})
	if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
		t.Fatalf("Expected OK from XGROUP CREATE MKSTREAM, got %v", res)
	}

	// Add events to newstream
	eng.ExecuteCommand("c1", []string{"XADD", "newstream", "*", "job", "email", "recipient", "alice@example.com"})
	res2 := eng.ExecuteCommand("c1", []string{"XADD", "newstream", "*", "job", "pdf", "recipient", "bob@example.com"})
	lastJobId := string(res2.Bulk)

	// 6. Test XREADGROUP with ">" (new messages)
	res = eng.ExecuteCommand("c1", []string{"XREADGROUP", "GROUP", "worker_grp", "consumer_a", "COUNT", "1", "STREAMS", "newstream", ">"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 1 {
		t.Fatalf("Expected XREADGROUP to return stream array, got %v", res)
	}
	deliveredItems := res.Array[0].Array[1].Array
	if len(deliveredItems) != 1 {
		t.Fatalf("Expected 1 delivered item, got %d", len(deliveredItems))
	}
	firstDeliveredId := string(deliveredItems[0].Array[0].Bulk)

	// 7. Test XPENDING Summary
	res = eng.ExecuteCommand("c1", []string{"XPENDING", "newstream", "worker_grp"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 4 || res.Array[0].Num != 1 {
		t.Fatalf("Expected XPENDING summary with 1 pending item, got %v", res)
	}

	// 8. Test XACK
	res = eng.ExecuteCommand("c1", []string{"XACK", "newstream", "worker_grp", firstDeliveredId})
	if res.Type != resp.IntegerPrefix || res.Num != 1 {
		t.Fatalf("Expected XACK to return 1, got %v", res)
	}

	// After XACK, pending should be 0
	res = eng.ExecuteCommand("c1", []string{"XPENDING", "newstream", "worker_grp"})
	if res.Array[0].Num != 0 {
		t.Fatalf("Expected 0 pending items after XACK, got %d", res.Array[0].Num)
	}

	// Consumer B reads remaining job
	res = eng.ExecuteCommand("c1", []string{"XREADGROUP", "GROUP", "worker_grp", "consumer_b", "COUNT", "1", "STREAMS", "newstream", ">"})
	bItems := res.Array[0].Array[1].Array
	if len(bItems) != 1 || string(bItems[0].Array[0].Bulk) != lastJobId {
		t.Fatalf("Expected consumer_b to read job %s, got %v", lastJobId, bItems)
	}

	// 9. Test XINFO STREAM & GROUPS
	res = eng.ExecuteCommand("c1", []string{"XINFO", "STREAM", "newstream"})
	if res.Type != resp.ArrayPrefix {
		t.Fatalf("Expected XINFO STREAM to return array, got %v", res)
	}

	res = eng.ExecuteCommand("c1", []string{"XINFO", "GROUPS", "newstream"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 1 {
		t.Fatalf("Expected 1 group in XINFO GROUPS, got %v", res)
	}

	// 10. Test XDEL & XTRIM
	res = eng.ExecuteCommand("c1", []string{"XDEL", "mystream", id2})
	if res.Type != resp.IntegerPrefix || res.Num != 1 {
		t.Fatalf("Expected XDEL to delete 1 item, got %v", res)
	}

	res = eng.ExecuteCommand("c1", []string{"XTRIM", "mystream", "MAXLEN", "1"})
	if res.Type != resp.IntegerPrefix || res.Num != 1 {
		t.Fatalf("Expected XTRIM to evict 1 item, got %v", res)
	}

	res = eng.ExecuteCommand("c1", []string{"XLEN", "mystream"})
	if res.Num != 1 {
		t.Fatalf("Expected XLEN to be 1 after trim, got %d", res.Num)
	}

	// 11. Test Blocking XREAD (XREAD BLOCK 300 STREAMS newstream $)
	readBlockedCompleted := make(chan bool)
	go func() {
		bRes := eng.ExecuteCommand("c2", []string{"XREAD", "BLOCK", "500", "STREAMS", "newstream", "$"})
		if bRes.Type == resp.ArrayPrefix && len(bRes.Array) == 1 {
			readBlockedCompleted <- true
		} else {
			readBlockedCompleted <- false
		}
	}()

	time.Sleep(50 * time.Millisecond)
	eng.ExecuteCommand("c1", []string{"XADD", "newstream", "*", "async_event", "unblocked"})

	select {
	case success := <-readBlockedCompleted:
		if !success {
			t.Fatalf("Blocked XREAD failed to receive async event")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for blocked XREAD")
	}

	// 12. Test Blocking XREADGROUP (XREADGROUP GROUP worker_grp consumer_c BLOCK 500 STREAMS newstream >)
	readGroupBlockedCompleted := make(chan bool)
	go func() {
		bRes := eng.ExecuteCommand("c3", []string{"XREADGROUP", "GROUP", "worker_grp", "consumer_c", "BLOCK", "500", "STREAMS", "newstream", ">"})
		if bRes.Type == resp.ArrayPrefix && len(bRes.Array) == 1 {
			readGroupBlockedCompleted <- true
		} else {
			readGroupBlockedCompleted <- false
		}
	}()

	time.Sleep(50 * time.Millisecond)
	eng.ExecuteCommand("c1", []string{"XADD", "newstream", "*", "group_event", "worker_unblocked"})

	select {
	case success := <-readGroupBlockedCompleted:
		if !success {
			t.Fatalf("Blocked XREADGROUP failed to receive async event")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for blocked XREADGROUP")
	}
}
