//go:build darwin

package reactor

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/GargAnshu9468/vortexkv/internal/engine"
)

// KqueueServer implements the event-driven reactor server using macOS kqueue/kevent.
type KqueueServer struct {
	cfg        Config
	listenerFd int
	workers    []*KqueueWorker
	roundRobin atomic.Uint64
	stopChan   chan struct{}
	wg         sync.WaitGroup
	connCount  atomic.Int64
}

// KqueueWorker manages a single kqueue instance pinned to an OS thread.
type KqueueWorker struct {
	id         int
	kqFd       int
	listenerFd int
	server     *KqueueServer
	conns      map[int]*Connection
	muConns    sync.RWMutex
	readBuf    []byte
	cmdArgs    []string
	localOps   int64
	stopChan   chan struct{}
}

// NewDarwinReactor creates a new kqueue-based reactor server for macOS.
func NewDarwinReactor(cfg Config) (*KqueueServer, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers()
	}
	if cfg.RingSize <= 0 {
		cfg.RingSize = 128 * 1024
	}

	return &KqueueServer{
		cfg:      cfg,
		stopChan: make(chan struct{}),
	}, nil
}

func (s *KqueueServer) Start() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("resolve addr: %w", err)
	}

	var sa syscall.Sockaddr
	var domain int
	if tcpAddr.IP == nil || tcpAddr.IP.To4() != nil {
		domain = syscall.AF_INET
		sa4 := &syscall.SockaddrInet4{Port: tcpAddr.Port}
		if tcpAddr.IP != nil {
			copy(sa4.Addr[:], tcpAddr.IP.To4())
		}
		sa = sa4
	} else {
		domain = syscall.AF_INET6
		sa6 := &syscall.SockaddrInet6{Port: tcpAddr.Port}
		copy(sa6.Addr[:], tcpAddr.IP.To16())
		sa = sa6
	}

	createListener := func() (int, error) {
		fd, err := syscall.Socket(domain, syscall.SOCK_STREAM, 0)
		if err != nil {
			return 0, fmt.Errorf("socket: %w", err)
		}
		if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
			_ = syscall.Close(fd)
			return 0, fmt.Errorf("setsockopt SO_REUSEADDR: %w", err)
		}
		_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0200, 1) // SO_REUSEPORT
		if err := syscall.SetNonblock(fd, true); err != nil {
			_ = syscall.Close(fd)
			return 0, fmt.Errorf("setnonblock listener: %w", err)
		}
		if err := syscall.Bind(fd, sa); err != nil {
			_ = syscall.Close(fd)
			return 0, fmt.Errorf("bind: %w", err)
		}
		if err := syscall.Listen(fd, 4096); err != nil {
			_ = syscall.Close(fd)
			return 0, fmt.Errorf("listen: %w", err)
		}
		return fd, nil
	}

	masterFd, err := createListener()
	if err != nil {
		return err
	}
	s.listenerFd = masterFd

	if tcpAddr.Port == 0 {
		if saBound, err := syscall.Getsockname(masterFd); err == nil {
			sa = saBound
		}
	}

	log.Printf("[VortexKV] ⚡ Event Reactor (kqueue) listening on %s (%d worker loops, SO_REUSEPORT mode)", s.cfg.Addr, s.cfg.Workers)

	// Initialize worker pool with independent listeners
	s.workers = make([]*KqueueWorker, s.cfg.Workers)
	for i := 0; i < s.cfg.Workers; i++ {
		kq, err := syscall.Kqueue()
		if err != nil {
			s.Stop()
			return fmt.Errorf("kqueue init: %w", err)
		}

		lFd := masterFd
		if i > 0 {
			wFd, err := createListener()
			if err == nil {
				lFd = wFd
			} else {
				lFd = 0
			}
		}

		worker := &KqueueWorker{
			id:         i,
			kqFd:       kq,
			listenerFd: lFd,
			server:     s,
			conns:      make(map[int]*Connection),
			readBuf:    make([]byte, 64*1024),
			cmdArgs:    make([]string, 0, 16),
			stopChan:   make(chan struct{}),
		}

		// Register listener in worker's kqueue
		if lFd > 0 {
			change := syscall.Kevent_t{
				Ident:  uint64(lFd),
				Filter: syscall.EVFILT_READ,
				Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
			}
			_, _ = syscall.Kevent(kq, []syscall.Kevent_t{change}, nil, nil)
		}

		s.workers[i] = worker
		s.wg.Add(1)
		go worker.run(&s.wg)
	}

	return nil
}

func (w *KqueueWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	// Let Go runtime M:N scheduler manage goroutines without forcing OS thread context switches

	events := make([]syscall.Kevent_t, 512)
	timeout := &syscall.Timespec{Sec: 0, Nsec: 1_000_000} // 1ms batching timeout

	for {
		select {
		case <-w.stopChan:
			return
		default:
		}

		nev, err := syscall.Kevent(w.kqFd, nil, events, timeout)
		if err != nil {
			select {
			case <-w.stopChan:
				return
			default:
			}
			if err == syscall.EINTR {
				continue
			}
			return
		}

		for i := 0; i < nev; i++ {
			ev := events[i]
			fd := int(ev.Ident)

			// Fast path: incoming connection on this worker's listener
			if w.listenerFd > 0 && fd == w.listenerFd {
				w.acceptConns()
				continue
			}

			w.muConns.RLock()
			conn, exists := w.conns[fd]
			w.muConns.RUnlock()

			if !exists || conn.Closed.Load() {
				continue
			}

			// Handle disconnect
			if ev.Flags&syscall.EV_EOF != 0 {
				w.closeConn(conn)
				continue
			}

			if ev.Filter == syscall.EVFILT_READ {
				w.handleRead(conn)
			}
		}
	}
}

func (w *KqueueWorker) acceptConns() {
	for {
		nfd, sa, err := syscall.Accept(w.listenerFd)
		if err != nil {
			break
		}

		if w.server.cfg.MaxClients > 0 && w.server.connCount.Load() >= w.server.cfg.MaxClients {
			_, _ = syscall.Write(nfd, []byte("-ERR max number of clients reached\r\n"))
			_ = syscall.Close(nfd)
			continue
		}

		_ = syscall.SetNonblock(nfd, true)
		_ = syscall.SetsockoptInt(nfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

		remoteIP := "unknown"
		if sa4, ok := sa.(*syscall.SockaddrInet4); ok {
			remoteIP = fmt.Sprintf("%d.%d.%d.%d:%d", sa4.Addr[0], sa4.Addr[1], sa4.Addr[2], sa4.Addr[3], sa4.Port)
		}

		connID := fmt.Sprintf("conn-%d", w.server.connCount.Add(1))
		w.server.cfg.Engine.Telemetry.IncrConnections()

		conn := &Connection{
			Fd:       nfd,
			ID:       connID,
			SubID:    w.id,
			InRing:   NewRingBuffer(w.server.cfg.RingSize),
			OutBuf:   make([]byte, 0, 64*1024),
			Session:  w.server.cfg.Engine.GetClientSession(connID),
			RemoteIP: remoteIP,
		}

		w.muConns.Lock()
		w.conns[nfd] = conn
		w.muConns.Unlock()

		wChange := syscall.Kevent_t{
			Ident:  uint64(nfd),
			Filter: syscall.EVFILT_READ,
			Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
		}
		_, _ = syscall.Kevent(w.kqFd, []syscall.Kevent_t{wChange}, nil, nil)
	}
}

func (w *KqueueWorker) incOps(eng *engine.Engine) {
	w.localOps++
	if w.localOps >= 64 {
		eng.Telemetry.AddTotalCommands(w.localOps)
		w.localOps = 0
	}
}

func (w *KqueueWorker) flushOps(eng *engine.Engine) {
	if w.localOps > 0 {
		eng.Telemetry.AddTotalCommands(w.localOps)
		w.localOps = 0
	}
}

func (w *KqueueWorker) executeCommand(conn *Connection, args []string) {
	if len(args) == 0 {
		return
	}

	eng := w.server.cfg.Engine
	isSimpleSession := (conn.Session == nil || (!conn.Session.InMulti && len(conn.Session.WatchedKeys) == 0)) &&
		eng.Password == "" &&
		(eng.Cluster == nil || !eng.Cluster.Enabled) &&
		(eng.Replication == nil || !eng.Replication.ReadOnly)

	cmdLen := len(args[0])
	switch cmdLen {
	case 3:
		switch AsCmd3(args[0]) {
		case CmdGet:
			if len(args) == 2 && isSimpleSession {
				ent, found := eng.Keyspace.Get(args[1])
				if !found {
					conn.OutBuf = append(conn.OutBuf, respNull...)
				} else if ent.Type == engine.TypeString {
					if s, ok := ent.Value.(string); ok {
						conn.OutBuf = AppendBulkString(conn.OutBuf, s)
					} else {
						res := eng.ExecuteCommandWithSession(conn.Session, conn.ID, args)
						conn.OutBuf = AppendValue(conn.OutBuf, res)
					}
				} else {
					res := eng.ExecuteCommandWithSession(conn.Session, conn.ID, args)
					conn.OutBuf = AppendValue(conn.OutBuf, res)
				}
				w.incOps(eng)
				return
			}
		case CmdSet:
			if len(args) == 3 && isSimpleSession && eng.AOF == nil && eng.Replication == nil && eng.MaxMemory == 0 && eng.WatchedCount() == 0 {
				eng.Keyspace.SetString(args[1], args[2])
				conn.OutBuf = append(conn.OutBuf, respOK...)
				w.incOps(eng)
				return
			}
		case CmdDel:
			if len(args) == 2 && isSimpleSession && eng.AOF == nil && eng.Replication == nil && eng.WatchedCount() == 0 {
				n := eng.Keyspace.Delete(args[1])
				if n > 0 {
					conn.OutBuf = append(conn.OutBuf, respOne...)
				} else {
					conn.OutBuf = append(conn.OutBuf, respZero...)
				}
				w.incOps(eng)
				return
			}
		}

	case 4:
		switch AsCmd4(args[0]) {
		case CmdPing:
			if len(args) == 1 {
				conn.OutBuf = append(conn.OutBuf, respPONG...)
				w.incOps(eng)
				return
			} else if len(args) == 2 {
				conn.OutBuf = AppendBulkString(conn.OutBuf, args[1])
				w.incOps(eng)
				return
			}
		case CmdIncr:
			if len(args) == 2 && isSimpleSession && eng.AOF == nil && eng.Replication == nil && eng.MaxMemory == 0 && eng.WatchedCount() == 0 {
				val, err := eng.Keyspace.IncrBy(args[1], 1)
				if err == nil {
					conn.OutBuf = append(conn.OutBuf, ':')
					conn.OutBuf = strconv.AppendInt(conn.OutBuf, val, 10)
					conn.OutBuf = append(conn.OutBuf, '\r', '\n')
					w.incOps(eng)
					return
				}
			}
		case CmdDecr:
			if len(args) == 2 && isSimpleSession && eng.AOF == nil && eng.Replication == nil && eng.MaxMemory == 0 && eng.WatchedCount() == 0 {
				val, err := eng.Keyspace.IncrBy(args[1], -1)
				if err == nil {
					conn.OutBuf = append(conn.OutBuf, ':')
					conn.OutBuf = strconv.AppendInt(conn.OutBuf, val, 10)
					conn.OutBuf = append(conn.OutBuf, '\r', '\n')
					w.incOps(eng)
					return
				}
			}
		case CmdQuit:
			conn.OutBuf = append(conn.OutBuf, respOK...)
			w.flushWrite(conn)
			w.closeConn(conn)
			return
		}
	}

	// Fallback to full engine execution
	res := eng.ExecuteCommandWithSession(conn.Session, conn.ID, args)
	conn.OutBuf = AppendValue(conn.OutBuf, res)
	w.incOps(eng)
}

func (w *KqueueWorker) handleRead(conn *Connection) {
	eng := w.server.cfg.Engine

	// ZERO-COPY FAST PATH: When InRing is empty, attempt to read directly and parse
	if conn.InRing.IsEmpty() {
		n, err := syscall.Read(conn.Fd, w.readBuf)
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				return
			}
			w.closeConn(conn)
			return
		}
		if n == 0 {
			w.closeConn(conn)
			return
		}

		buf := w.readBuf[:n]
		args, consumed, err := ParseCommandInto(buf, w.cmdArgs)
		if err != nil {
			conn.OutBuf = append(conn.OutBuf, []byte("-ERR protocol error\r\n")...)
			w.flushWrite(conn)
			w.closeConn(conn)
			return
		}

		if consumed > 0 && args != nil {
			w.executeCommand(conn, args)

			if consumed == n {
				// Perfect single command! Zero memory copy into ring buffer.
				w.flushOps(eng)
				if len(conn.OutBuf) > 0 {
					w.flushWrite(conn)
				}
				return
			}

			// Pipelined or multiple commands in w.readBuf
			buf = buf[consumed:]
			for len(buf) > 0 {
				args, consumed, err = ParseCommandInto(buf, w.cmdArgs)
				if err != nil {
					conn.OutBuf = append(conn.OutBuf, []byte("-ERR protocol error\r\n")...)
					w.flushWrite(conn)
					w.closeConn(conn)
					return
				}
				if consumed == 0 || args == nil {
					// Incomplete trailing bytes, buffer into InRing
					_, _ = conn.InRing.Write(buf)
					break
				}
				w.executeCommand(conn, args)
				buf = buf[consumed:]
			}

			w.flushOps(eng)
			if len(conn.OutBuf) > 0 {
				w.flushWrite(conn)
			}
			return
		}

		// Incomplete initial command, write to InRing and wait for more data
		_, _ = conn.InRing.Write(buf)
	} else {
		// InRing has existing buffered data: read all available bytes into InRing
		for {
			n, err := syscall.Read(conn.Fd, w.readBuf)
			if err != nil {
				if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
					break
				}
				w.closeConn(conn)
				return
			}
			if n == 0 {
				w.closeConn(conn)
				return
			}

			_, _ = conn.InRing.Write(w.readBuf[:n])
			if n < len(w.readBuf) {
				break
			}
		}
	}

	// Drain and execute all commands present in InRing
	for {
		data := conn.InRing.Peek()
		if conn.InRing.Len() > len(data) {
			data = conn.InRing.Bytes()
		}
		if len(data) == 0 {
			break
		}

		args, consumed, err := ParseCommandInto(data, w.cmdArgs)
		if err != nil {
			conn.OutBuf = append(conn.OutBuf, []byte("-ERR protocol error\r\n")...)
			w.flushWrite(conn)
			w.closeConn(conn)
			return
		}
		if consumed == 0 || args == nil {
			break // Incomplete command, wait for next event
		}

		conn.InRing.AdvanceRead(consumed)
		w.executeCommand(conn, args)
	}

	if conn.InRing.IsEmpty() {
		conn.InRing.Reset()
	}

	w.flushOps(eng)

	// Flush pipelined batch output
	if len(conn.OutBuf) > 0 {
		w.flushWrite(conn)
	}
}

func (w *KqueueWorker) flushWrite(conn *Connection) {
	buf := conn.OutBuf
	for len(buf) > 0 {
		n, err := syscall.Write(conn.Fd, buf)
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				for retry := 0; retry < 50; retry++ {
					n, err = syscall.Write(conn.Fd, buf)
					if err == nil {
						buf = buf[n:]
						break
					}
					if err != syscall.EAGAIN && err != syscall.EWOULDBLOCK {
						w.closeConn(conn)
						return
					}
				}
				if len(buf) == 0 {
					break
				}
				copy(conn.OutBuf, buf)
				conn.OutBuf = conn.OutBuf[:len(buf)]
				return
			}
			w.closeConn(conn)
			return
		}
		buf = buf[n:]
	}
	conn.OutBuf = conn.OutBuf[:0]
}

func (w *KqueueWorker) closeConn(conn *Connection) {
	if conn.Closed.CompareAndSwap(false, true) {
		w.muConns.Lock()
		delete(w.conns, conn.Fd)
		w.muConns.Unlock()

		// Delete from kqueue
		del := syscall.Kevent_t{
			Ident:  uint64(conn.Fd),
			Filter: syscall.EVFILT_READ,
			Flags:  syscall.EV_DELETE,
		}
		_, _ = syscall.Kevent(w.kqFd, []syscall.Kevent_t{del}, nil, nil)
		_ = syscall.Close(conn.Fd)

		w.server.connCount.Add(-1)
		w.server.cfg.Engine.ClearClientSession(conn.ID)
		w.server.cfg.Engine.Telemetry.DecrConnections()
	}
}

func (s *KqueueServer) Stop() error {
	select {
	case <-s.stopChan:
		return nil
	default:
		close(s.stopChan)
	}

	if s.listenerFd > 0 {
		_ = syscall.Close(s.listenerFd)
	}

	for _, w := range s.workers {
		if w != nil {
			close(w.stopChan)
			if w.listenerFd > 0 && w.listenerFd != s.listenerFd {
				_ = syscall.Close(w.listenerFd)
			}
			var connsToClose []*Connection
			w.muConns.Lock()
			for _, c := range w.conns {
				connsToClose = append(connsToClose, c)
			}
			w.muConns.Unlock()
			for _, c := range connsToClose {
				w.closeConn(c)
			}
			if w.kqFd > 0 {
				_ = syscall.Close(w.kqFd)
			}
		}
	}

	s.wg.Wait()
	return nil
}
