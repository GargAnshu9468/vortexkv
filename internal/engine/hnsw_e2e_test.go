package engine

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/vortexkv/vortexkv/internal/persistence"
	"github.com/vortexkv/vortexkv/internal/resp"
)

func TestHNSWEngineE2E(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// 1. Ingest 50 8-dimensional vectors
	dim := 8
	numVectors := 50
	rnd := rand.New(rand.NewSource(1337))

	for i := 0; i < numVectors; i++ {
		args := []string{"VADD", "doc_embeddings", fmt.Sprintf("doc_%d", i)}
		for d := 0; d < dim; d++ {
			args = append(args, fmt.Sprintf("%f", rnd.Float32()))
		}
		res := eng.ExecuteCommand("c1", args)
		if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
			t.Fatalf("Failed VADD: %v", res)
		}
	}

	// 2. Query VINFO
	infoRes := eng.ExecuteCommand("c1", []string{"VINFO", "doc_embeddings"})
	if infoRes.Type != resp.ArrayPrefix || len(infoRes.Array) < 4 {
		t.Fatalf("Expected array from VINFO, got: %v", infoRes)
	}
	t.Logf("VINFO response: %d items", len(infoRes.Array))

	// 3. Search with VSEARCH (standard and with EF)
	qArgs := []string{"VSEARCH", "doc_embeddings", "5", "cosine"}
	for d := 0; d < dim; d++ {
		qArgs = append(qArgs, fmt.Sprintf("%f", rnd.Float32()))
	}
	searchRes := eng.ExecuteCommand("c1", qArgs)
	if searchRes.Type != resp.ArrayPrefix || len(searchRes.Array) != 5 {
		t.Fatalf("Expected 5 search results, got %d (%v)", len(searchRes.Array), searchRes)
	}

	// Search with EF 32
	qArgsEF := append(qArgs, "EF", "32")
	searchResEF := eng.ExecuteCommand("c1", qArgsEF)
	if searchResEF.Type != resp.ArrayPrefix || len(searchResEF.Array) != 5 {
		t.Fatalf("Expected 5 search results with EF, got %d (%v)", len(searchResEF.Array), searchResEF)
	}

	// 4. Test VSIM between two documents
	simRes := eng.ExecuteCommand("c1", []string{"VSIM", "doc_embeddings", "doc_1", "doc_2", "cosine"})
	if simRes.Type != resp.BulkStringPrefix {
		t.Fatalf("Expected bulk string from VSIM, got: %v", simRes)
	}
	simScore, err := strconv.ParseFloat(string(simRes.Bulk), 64)
	if err != nil || simScore < -1.0 || simScore > 1.0 {
		t.Fatalf("Invalid similarity score: %v", simRes)
	}

	// 5. Test VDEL
	delRes := eng.ExecuteCommand("c1", []string{"VDEL", "doc_embeddings", "doc_1", "doc_2"})
	if delRes.Type != resp.IntegerPrefix || delRes.Num != 2 {
		t.Fatalf("Expected VDEL to return 2, got: %v", delRes)
	}

	// Verify count decreased in VINFO
	infoRes2 := eng.ExecuteCommand("c1", []string{"VINFO", "doc_embeddings"})
	for i := 0; i < len(infoRes2.Array); i += 2 {
		if string(infoRes2.Array[i].Bulk) == "count" {
			if string(infoRes2.Array[i+1].Bulk) != "48" {
				t.Fatalf("Expected count 48 after delete, got %s", string(infoRes2.Array[i+1].Bulk))
			}
		}
	}
}
