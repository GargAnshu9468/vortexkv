package datastruct

import (
	"testing"
	"time"
)

func TestStreamOperations(t *testing.T) {
	s := NewStream()

	// 1. Test Add
	id1, err := s.Add("1000-0", map[string]string{"sensor": "temp", "val": "21.5"}, []string{"sensor", "val"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if id1 != "1000-0" {
		t.Fatalf("Expected 1000-0, got %s", id1)
	}

	id2, err := s.Add("1000-1", map[string]string{"sensor": "temp", "val": "22.0"}, []string{"sensor", "val"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if id2 != "1000-1" {
		t.Fatalf("Expected 1000-1, got %s", id2)
	}

	id3, err := s.Add("1001-0", map[string]string{"sensor": "press", "val": "1013"}, []string{"sensor", "val"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if s.Len() != 3 {
		t.Fatalf("Expected len 3, got %d", s.Len())
	}

	// 2. Test Range & RevRange
	r := s.Range("-", "+", -1)
	if len(r) != 3 || r[0].ID != "1000-0" || r[2].ID != id3 {
		t.Fatalf("Range failed: %+v", r)
	}

	rev := s.RevRange("+", "-", -1)
	if len(rev) != 3 || rev[0].ID != id3 || rev[2].ID != "1000-0" {
		t.Fatalf("RevRange failed: %+v", rev)
	}

	// 3. Test Trim
	trimmed := s.Trim(2)
	if trimmed != 1 || s.Len() != 2 {
		t.Fatalf("Trim failed: trimmed %d, len %d", trimmed, s.Len())
	}

	// 4. Test Delete
	deleted := s.Delete([]string{id2})
	if deleted != 1 || s.Len() != 1 {
		t.Fatalf("Delete failed: deleted %d, len %d", deleted, s.Len())
	}
}

func TestStreamConsumerGroups(t *testing.T) {
	s := NewStream()

	// Seed items
	id1, _ := s.Add("1000-0", map[string]string{"job": "email_welcome"}, []string{"job"})
	id2, _ := s.Add("1000-1", map[string]string{"job": "invoice_pdf"}, []string{"job"})
	id3, _ := s.Add("1000-2", map[string]string{"job": "slack_alert"}, []string{"job"})

	// 1. Create Group starting from 0
	err := s.CreateGroup("workers", "0-0")
	if err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}

	// Duplicate create should error
	if err := s.CreateGroup("workers", "0-0"); err != ErrGroupExists {
		t.Fatalf("Expected ErrGroupExists, got %v", err)
	}

	// 2. ReadGroup with ">" (undelivered messages)
	// Worker 1 reads 2 items
	w1Items, err := s.ReadGroup("workers", "worker_1", ">", 2, false)
	if err != nil {
		t.Fatalf("ReadGroup worker_1 failed: %v", err)
	}
	if len(w1Items) != 2 || w1Items[0].ID != id1 || w1Items[1].ID != id2 {
		t.Fatalf("Worker 1 unexpected items: %+v", w1Items)
	}

	// Worker 2 reads remaining items
	w2Items, err := s.ReadGroup("workers", "worker_2", ">", 2, false)
	if err != nil {
		t.Fatalf("ReadGroup worker_2 failed: %v", err)
	}
	if len(w2Items) != 1 || w2Items[0].ID != id3 {
		t.Fatalf("Worker 2 unexpected items: %+v", w2Items)
	}

	// Worker 1 reading ">" again should return 0 (all delivered)
	emptyItems, err := s.ReadGroup("workers", "worker_1", ">", 10, false)
	if err != nil || len(emptyItems) != 0 {
		t.Fatalf("Expected 0 new items, got %d", len(emptyItems))
	}

	// 3. Inspect Pending Summary
	total, minID, maxID, counts, err := s.GetPendingSummary("workers")
	if err != nil || total != 3 || minID != id1 || maxID != id3 || counts["worker_1"] != 2 || counts["worker_2"] != 1 {
		t.Fatalf("Unexpected pending summary: total=%d, min=%s, max=%s, counts=%+v", total, minID, maxID, counts)
	}

	// 4. Inspect Detailed Pending
	detailed, err := s.GetPendingDetailed("workers", "-", "+", 10, "", 0)
	if err != nil || len(detailed) != 3 {
		t.Fatalf("Unexpected detailed pending: %+v", detailed)
	}

	// 5. Acknowledge items (XACK)
	acked := s.Ack("workers", []string{id1, id3})
	if acked != 2 {
		t.Fatalf("Expected 2 acked, got %d", acked)
	}

	// Pending should now be 1 (id2 owned by worker_1)
	totalAfterAck, _, _, countsAfterAck, _ := s.GetPendingSummary("workers")
	if totalAfterAck != 1 || countsAfterAck["worker_1"] != 1 {
		t.Fatalf("Expected 1 pending after ack, got %d", totalAfterAck)
	}

	// 6. Worker 1 reads pending messages (ID != ">")
	pendingReplay, err := s.ReadGroup("workers", "worker_1", "0-0", 10, false)
	if err != nil || len(pendingReplay) != 1 || pendingReplay[0].ID != id2 {
		t.Fatalf("Expected 1 pending replay item id2, got: %+v", pendingReplay)
	}

	// 7. Test Waiter for blocking read
	waitCh, cancel := s.RegisterWaiter()
	defer cancel()

	itemAdded := false
	go func() {
		time.Sleep(50 * time.Millisecond)
		itemAdded = true
		s.Add("*", map[string]string{"job": "new_event"}, []string{"job"})
	}()

	select {
	case <-waitCh:
		if !itemAdded {
			t.Fatalf("Woke up before item was added")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Timed out waiting for stream write")
	}
}
