package server

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vortexkv/vortexkv/internal/engine"
	"github.com/vortexkv/vortexkv/internal/pubsub"
	"github.com/vortexkv/vortexkv/internal/resp"
)

type TCPServer struct {
	addr       string
	listener   net.Listener
	engine     *engine.Engine
	stopChan   chan struct{}
	wg         sync.WaitGroup
	connCount  atomic.Int64
	MaxClients int64
	TLSConfig  *tls.Config
}

func NewTCPServer(addr string, eng *engine.Engine) *TCPServer {
	return &TCPServer{
		addr:       addr,
		engine:     eng,
		stopChan:   make(chan struct{}),
		MaxClients: 10000,
	}
}

func (s *TCPServer) Start() error {
	var l net.Listener
	var err error

	if s.TLSConfig != nil {
		l, err = tls.Listen("tcp", s.addr, s.TLSConfig)
		if err != nil {
			return err
		}
		log.Printf("[VortexKV] 🔒 TLS-Encrypted Redis RESP Server listening on %s", s.addr)
	} else {
		l, err = net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}
		log.Printf("[VortexKV] 🚀 Redis RESP Wire Server listening on %s", s.addr)
	}
	s.listener = l

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *TCPServer) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return
			default:
				log.Printf("[VortexKV] Accept error: %v", err)
				continue
			}
		}

		// Enforce connection ceiling
		snap := s.engine.Telemetry.GetSnapshot(0)
		if s.MaxClients > 0 && snap.ActiveConnections >= s.MaxClients {
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = conn.Close()
			continue
		}

		// Set TCP keep-alive and high-velocity buffer tuning
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
			_ = tcpConn.SetReadBuffer(128 * 1024)
			_ = tcpConn.SetWriteBuffer(128 * 1024)
		}

		connID := fmt.Sprintf("client-%d", s.connCount.Add(1))
		s.engine.Telemetry.IncrConnections()

		go s.handleConnection(connID, conn)
	}
}

func (s *TCPServer) handleConnection(connID string, conn net.Conn) {
	defer func() {
		conn.Close()
		s.engine.ClearClientSession(connID)
		s.engine.Telemetry.DecrConnections()
	}()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	session := s.engine.GetClientSession(connID)

	// Per-connection pubsub state
	var subChans []string
	var subQueue pubsub.Subscriber
	var listeningPort int

	cleanupPubSub := func() {
		if subQueue != nil {
			s.engine.Broker.UnsubscribeAll(subQueue)
			subQueue = nil
			subChans = nil
		}
	}
	defer cleanupPubSub()

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		cmdUpper := engine.ToUpperFast(args[0])

		// Handle QUIT command
		if cmdUpper == "QUIT" {
			_ = writer.WriteOK()
			_ = writer.Flush()
			return
		}

		// Track replica listening port
		if cmdUpper == "REPLCONF" && len(args) >= 3 && strings.ToLower(args[1]) == "listening-port" {
			if p, err := strconv.Atoi(args[2]); err == nil {
				listeningPort = p
			}
		}

		// Handle PSYNC / SYNC replication handshake
		if cmdUpper == "PSYNC" || cmdUpper == "SYNC" {
			if s.engine.Password != "" && !session.Authenticated {
				_ = writer.WriteError("NOAUTH Authentication required.")
				_ = writer.Flush()
				continue
			}

			if s.engine.Replication != nil {
				_ = s.engine.Replication.HandlePSync(conn, conn.RemoteAddr().String(), listeningPort)
				return
			}
		}

		// Handle SUBSCRIBE
		if cmdUpper == "SUBSCRIBE" {
			// Check authentication first if password required
			if s.engine.Password != "" && !session.Authenticated {
				_ = writer.WriteError("NOAUTH Authentication required.")
				_ = writer.Flush()
				continue
			}

			if len(args) < 2 {
				_ = writer.WriteError("ERR wrong number of arguments for 'subscribe'")
				_ = writer.Flush()
				continue
			}

			if subQueue == nil {
				subQueue = make(pubsub.Subscriber, 256)
			}

			for _, chName := range args[1:] {
				s.engine.Broker.Subscribe(chName, subQueue)
				subChans = append(subChans, chName)

				_ = writer.WriteArrayHeader(3)
				_ = writer.WriteBulkString("subscribe")
				_ = writer.WriteBulkString(chName)
				_ = writer.WriteInteger(int64(len(subChans)))
			}
			_ = writer.Flush()

			// Enter subscription pump loop for this connection
			s.handleSubscribedClient(conn, reader, writer, subQueue, &subChans)
			return
		}

		// Execute regular command with pre-bound session
		response := s.engine.ExecuteCommandWithSession(session, connID, args)
		if err := writer.WriteValue(response); err != nil {
			return
		}

		// Smart pipeline write coalescing: only flush when read buffer is empty
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *TCPServer) handleSubscribedClient(
	conn net.Conn,
	reader *resp.Reader,
	writer *resp.Writer,
	subQueue pubsub.Subscriber,
	subChans *[]string,
) {
	doneChan := make(chan struct{})
	defer close(doneChan)

	// Incoming client command reader goroutine
	clientCmdChan := make(chan []string, 10)
	clientErrChan := make(chan error, 1)

	go func() {
		for {
			cmd, err := reader.ReadCommand()
			if err != nil {
				select {
				case clientErrChan <- err:
				case <-doneChan:
				}
				return
			}
			select {
			case clientCmdChan <- cmd:
			case <-doneChan:
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-subQueue:
			if !ok {
				return
			}
			// Write message array: ["message", channel, data]
			_ = writer.WriteArrayHeader(3)
			_ = writer.WriteBulkString("message")
			_ = writer.WriteBulkString("channel")
			_ = writer.WriteBulkBytes(msg)
			_ = writer.Flush()

		case cmd := <-clientCmdChan:
			if len(cmd) == 0 {
				continue
			}
			cmdUpper := strings.ToUpper(cmd[0])
			switch cmdUpper {
			case "SUBSCRIBE":
				for _, ch := range cmd[1:] {
					s.engine.Broker.Subscribe(ch, subQueue)
					*subChans = append(*subChans, ch)
					_ = writer.WriteArrayHeader(3)
					_ = writer.WriteBulkString("subscribe")
					_ = writer.WriteBulkString(ch)
					_ = writer.WriteInteger(int64(len(*subChans)))
				}
				_ = writer.Flush()
			case "UNSUBSCRIBE":
				// unsubscribe logic
				_ = writer.WriteArrayHeader(3)
				_ = writer.WriteBulkString("unsubscribe")
				_ = writer.WriteBulkString("all")
				_ = writer.WriteInteger(0)
				_ = writer.Flush()
			case "PING":
				_ = writer.WriteArrayHeader(2)
				_ = writer.WriteBulkString("pong")
				_ = writer.WriteBulkString("")
				_ = writer.Flush()
			case "QUIT":
				return
			default:
				_ = writer.WriteError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context")
				_ = writer.Flush()
			}

		case <-clientErrChan:
			return
		}
	}
}

func (s *TCPServer) Stop() error {
	close(s.stopChan)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}
