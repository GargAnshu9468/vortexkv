package reactor

import (
	"errors"
	"io"
)

var (
	ErrBufferFull  = errors.New("reactor: ring buffer full")
	ErrBufferEmpty = errors.New("reactor: ring buffer empty")
)

// RingBuffer provides a high-throughput, contiguous circular byte buffer
// optimized for network socket event loops with zero runtime heap allocations.
type RingBuffer struct {
	buf  []byte
	size int
	r    int // read cursor
	w    int // write cursor
}

// NewRingBuffer allocates a circular buffer with the specified capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 64 * 1024 // 64KB default
	}
	return &RingBuffer{
		buf:  make([]byte, capacity),
		size: capacity,
	}
}

// Len returns the number of unread readable bytes in the buffer.
func (b *RingBuffer) Len() int {
	if b.w >= b.r {
		return b.w - b.r
	}
	return b.size - b.r + b.w
}

// Cap returns the total capacity of the buffer.
func (b *RingBuffer) Cap() int {
	return b.size
}

// Free returns the number of bytes that can be written before full.
func (b *RingBuffer) Free() int {
	return b.size - 1 - b.Len()
}

// IsEmpty returns true if there are no readable bytes.
func (b *RingBuffer) IsEmpty() bool {
	return b.r == b.w
}

// IsFull returns true if the buffer cannot accept any more bytes without overwriting.
func (b *RingBuffer) IsFull() bool {
	return b.Free() == 0
}

// Reset discards all unread bytes and resets cursors.
func (b *RingBuffer) Reset() {
	b.r = 0
	b.w = 0
}

// Write appends bytes from p into the circular buffer.
func (b *RingBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	if n > b.Free() {
		// Expand buffer to accommodate surge traffic
		b.grow(b.Len() + n + 1)
	}

	if b.w >= b.r {
		c1 := b.size - b.w
		if n <= c1 {
			copy(b.buf[b.w:], p)
			b.w += n
			if b.w == b.size {
				b.w = 0
			}
			return n, nil
		}
		copy(b.buf[b.w:], p[:c1])
		c2 := n - c1
		copy(b.buf[:c2], p[c1:])
		b.w = c2
		return n, nil
	}

	copy(b.buf[b.w:], p)
	b.w += n
	return n, nil
}

// Read copies up to len(p) readable bytes into p and advances the read cursor.
func (b *RingBuffer) Read(p []byte) (int, error) {
	if b.IsEmpty() {
		return 0, io.EOF
	}
	n := len(p)
	avail := b.Len()
	if n > avail {
		n = avail
	}

	if b.w > b.r {
		copy(p, b.buf[b.r:b.r+n])
		b.r += n
		return n, nil
	}

	c1 := b.size - b.r
	if n <= c1 {
		copy(p, b.buf[b.r:b.r+n])
		b.r += n
		if b.r == b.size {
			b.r = 0
		}
		return n, nil
	}

	copy(p[:c1], b.buf[b.r:])
	c2 := n - c1
	copy(p[c1:], b.buf[:c2])
	b.r = c2
	return n, nil
}

// Peek returns all contiguous readable bytes from the current read cursor without advancing.
// If the buffer wraps around, Peek returns the first contiguous segment until the physical end of the slice.
func (b *RingBuffer) Peek() []byte {
	if b.IsEmpty() {
		return nil
	}
	if b.w > b.r {
		return b.buf[b.r:b.w]
	}
	return b.buf[b.r:b.size]
}

// Bytes returns all readable bytes linearly. If wrapped, it copies into a linear slice.
func (b *RingBuffer) Bytes() []byte {
	avail := b.Len()
	if avail == 0 {
		return nil
	}
	if b.w > b.r {
		return b.buf[b.r:b.w]
	}
	out := make([]byte, avail)
	c1 := b.size - b.r
	copy(out[:c1], b.buf[b.r:])
	copy(out[c1:], b.buf[:b.w])
	return out
}

// AdvanceRead advances the read cursor by n bytes.
func (b *RingBuffer) AdvanceRead(n int) {
	if n <= 0 {
		return
	}
	avail := b.Len()
	if n > avail {
		n = avail
	}
	b.r = (b.r + n) % b.size
}

// AdvanceWrite advances the write cursor by n bytes after direct socket write.
func (b *RingBuffer) AdvanceWrite(n int) {
	if n <= 0 {
		return
	}
	b.w = (b.w + n) % b.size
}

// WriteSlice returns the contiguous writeable slice directly pointing into the underlying array,
// allowing a zero-copy syscall.Read() directly into the ring buffer.
func (b *RingBuffer) WriteSlice() []byte {
	if b.w >= b.r {
		if b.r == 0 {
			return b.buf[b.w : b.size-1]
		}
		return b.buf[b.w:b.size]
	}
	return b.buf[b.w : b.r-1]
}

func (b *RingBuffer) grow(minCap int) {
	newCap := b.size * 2
	if newCap < minCap {
		newCap = minCap * 2
	}
	newBuf := make([]byte, newCap)
	avail := b.Len()
	if avail > 0 {
		if b.w > b.r {
			copy(newBuf, b.buf[b.r:b.w])
		} else {
			c1 := b.size - b.r
			copy(newBuf[:c1], b.buf[b.r:])
			copy(newBuf[c1:], b.buf[:b.w])
		}
	}
	b.buf = newBuf
	b.size = newCap
	b.r = 0
	b.w = avail
}
