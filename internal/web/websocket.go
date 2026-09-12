package web

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
)

const (
	wsMagicGUID    = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	maxWSFrameSize = 16 * 1024 * 1024 // 16MB max WebSocket frame to prevent memory exhaustion
)

type WSConn struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex
}

func UpgradeWebSocket(w http.ResponseWriter, r *http.Request) (*WSConn, error) {
	if r.Header.Get("Upgrade") != "websocket" && r.Header.Get("upgrade") != "websocket" {
		return nil, errors.New("not a websocket handshake")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key")
	}

	h := sha1.New()
	h.Write([]byte(key + wsMagicGUID))
	acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("webserver doesn't support hijacking")
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	res := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"

	if _, err := bufrw.WriteString(res); err != nil {
		conn.Close()
		return nil, err
	}
	if err := bufrw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	return &WSConn{conn: conn, reader: bufrw.Reader}, nil
}

func (ws *WSConn) WriteText(msg []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	var frame []byte
	frame = append(frame, 0x81) // FIN + text frame

	length := len(msg)
	if length <= 125 {
		frame = append(frame, byte(length))
	} else if length <= 65535 {
		frame = append(frame, 126, byte(length>>8), byte(length&0xFF))
	} else {
		frame = append(frame, 127,
			byte(length>>56), byte(length>>48), byte(length>>40), byte(length>>32),
			byte(length>>24), byte(length>>16), byte(length>>8), byte(length&0xFF),
		)
	}

	frame = append(frame, msg...)
	_, err := ws.conn.Write(frame)
	return err
}

func (ws *WSConn) ReadMessage() ([]byte, error) {
	reader := ws.reader
	if reader == nil {
		reader = bufio.NewReader(ws.conn)
		ws.reader = reader
	}

	b0, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	opcode := b0 & 0x0F
	if opcode == 0x08 { // Close frame
		return nil, io.EOF
	}

	b1, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	masked := (b1 & 0x80) != 0
	payloadLen := int(b1 & 0x7F)

	if payloadLen == 126 {
		lenBuf := make([]byte, 2)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return nil, err
		}
		payloadLen = int(lenBuf[0])<<8 | int(lenBuf[1])
	} else if payloadLen == 127 {
		lenBuf := make([]byte, 8)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return nil, err
		}
		payloadLen = int(lenBuf[4])<<24 | int(lenBuf[5])<<16 | int(lenBuf[6])<<8 | int(lenBuf[7])
	}

	if payloadLen < 0 || payloadLen > maxWSFrameSize {
		return nil, errors.New("websocket frame payload exceeds maximum allowed size")
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(reader, mask[:]); err != nil {
			return nil, err
		}
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}

	if masked {
		for i := 0; i < payloadLen; i++ {
			payload[i] ^= mask[i%4]
		}
	}

	return payload, nil
}

func (ws *WSConn) Close() error {
	return ws.conn.Close()
}
