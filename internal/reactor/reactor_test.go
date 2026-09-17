package reactor

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexkv/internal/engine"
)

func TestReactor_EndToEnd(t *testing.T) {
	// 1. Setup in-memory engine
	eng, err := engine.NewEngine("", "")
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()
	eng.Password = "test_pass"
	port := 17399
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := NewServer(Config{
		Addr:       addr,
		Engine:     eng,
		Workers:    2,
		RingSize:   64 * 1024,
		EngineType: "auto",
	})
	if err != nil {
		t.Fatalf("failed to create reactor server: %v", err)
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start reactor server: %v", err)
	}
	defer srv.Stop()

	// Allow server to listen
	time.Sleep(50 * time.Millisecond)

	// 2. Connect client
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 3. Test AUTH
	_, _ = conn.Write([]byte("*2\r\n$4\r\nAUTH\r\n$9\r\ntest_pass\r\n"))
	line, err := reader.ReadString('\n')
	if err != nil || line != "+OK\r\n" {
		t.Fatalf("auth failed: %v, got: %q", err, line)
	}

	// 4. Test PING
	_, _ = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	line, err = reader.ReadString('\n')
	if err != nil || line != "+PONG\r\n" {
		t.Fatalf("ping failed: %v, got: %q", err, line)
	}

	// 5. Test SET & GET
	_, _ = conn.Write([]byte("*3\r\n$3\r\nSET\r\n$4\r\nhero\r\n$6\r\nvortex\r\n"))
	line, err = reader.ReadString('\n')
	if err != nil || line != "+OK\r\n" {
		t.Fatalf("set failed: %v, got: %q", err, line)
	}

	_, _ = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$4\r\nhero\r\n"))
	line, err = reader.ReadString('\n') // $6\r\n
	if err != nil || line != "$6\r\n" {
		t.Fatalf("get header failed: %v, got: %q", err, line)
	}
	val, err := reader.ReadString('\n') // vortex\r\n
	if err != nil || val != "vortex\r\n" {
		t.Fatalf("get val failed: %v, got: %q", err, val)
	}

	// 6. Test Pipelined Batch (5 commands at once)
	batch := []byte(
		"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n" +
			"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n" +
			"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n" +
			"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n" +
			"*1\r\n$4\r\nPING\r\n",
	)
	_, _ = conn.Write(batch)

	resp1, _ := reader.ReadString('\n') // +OK\r\n
	resp2, _ := reader.ReadString('\n') // +OK\r\n
	resp3H, _ := reader.ReadString('\n')
	resp3V, _ := reader.ReadString('\n')
	resp4H, _ := reader.ReadString('\n')
	resp4V, _ := reader.ReadString('\n')
	resp5, _ := reader.ReadString('\n') // +PONG\r\n

	if resp1 != "+OK\r\n" || resp2 != "+OK\r\n" || resp3H != "$2\r\n" || resp3V != "v1\r\n" ||
		resp4H != "$2\r\n" || resp4V != "v2\r\n" || resp5 != "+PONG\r\n" {
		t.Fatalf("pipelined batch mismatch: %q, %q, %q, %q, %q, %q, %q",
			resp1, resp2, resp3H, resp3V, resp4H, resp4V, resp5)
	}
}
