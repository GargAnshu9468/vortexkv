package reactor

import (
	"bytes"
	"errors"
)

var (
	ErrIncomplete = errors.New("reactor: incomplete RESP command")
	ErrProtocol   = errors.New("reactor: protocol error")
)

// ParseCommand parses a single RESP or inline command from the provided byte slice.
func ParseCommand(data []byte) ([]string, int, error) {
	return ParseCommandInto(data, nil)
}

// ParseCommandInto parses a command appending tokens into dst to avoid slice allocations.
func ParseCommandInto(data []byte, dst []string) ([]string, int, error) {
	if len(data) == 0 {
		return nil, 0, nil
	}

	// 1. Check for RESP Array format: *<count>\r\n
	if data[0] == '*' {
		return parseRESPArrayInto(data, dst)
	}

	// 2. Fallback to Inline command format: <cmd> <arg1> ...\r\n
	return parseInlineCommandInto(data, dst)
}

func parseRESPArrayInto(data []byte, dst []string) ([]string, int, error) {
	idx := bytes.IndexByte(data, '\n')
	if idx == -1 {
		return nil, 0, nil // Incomplete
	}
	if idx < 2 || data[idx-1] != '\r' {
		return nil, 0, ErrProtocol
	}

	// Fast integer parse for count
	count := 0
	for i := 1; i < idx-1; i++ {
		c := data[i]
		if c < '0' || c > '9' {
			return nil, 0, ErrProtocol
		}
		count = count*10 + int(c-'0')
	}
	if count == 0 {
		return dst[:0], idx + 1, nil
	}

	args := dst[:0]
	if cap(args) < count {
		args = make([]string, 0, count)
	}
	consumed := idx + 1

	for i := 0; i < count; i++ {
		if consumed >= len(data) {
			return nil, 0, nil // Incomplete
		}

		if data[consumed] != '$' {
			return nil, 0, ErrProtocol
		}

		nextIdx := bytes.IndexByte(data[consumed:], '\n')
		if nextIdx == -1 {
			return nil, 0, nil // Incomplete
		}
		endLen := consumed + nextIdx
		if endLen < consumed+2 || data[endLen-1] != '\r' {
			return nil, 0, ErrProtocol
		}

		// Fast integer parse for bulk length
		bulkLen := 0
		start := consumed + 1
		isNegative := false
		if start < endLen-1 && data[start] == '-' {
			isNegative = true
			start++
		}
		for j := start; j < endLen-1; j++ {
			c := data[j]
			if c < '0' || c > '9' {
				return nil, 0, ErrProtocol
			}
			bulkLen = bulkLen*10 + int(c-'0')
		}
		if isNegative {
			bulkLen = -bulkLen
		}

		consumed = endLen + 1

		if bulkLen == -1 {
			args = append(args, "")
			continue
		}
		if bulkLen < 0 {
			return nil, 0, ErrProtocol
		}

		// Ensure full payload + \r\n is present
		if len(data[consumed:]) < bulkLen+2 {
			return nil, 0, nil // Incomplete
		}

		if data[consumed+bulkLen] != '\r' || data[consumed+bulkLen+1] != '\n' {
			return nil, 0, ErrProtocol
		}

		args = append(args, string(data[consumed:consumed+bulkLen]))
		consumed += bulkLen + 2
	}

	return args, consumed, nil
}

func parseInlineCommandInto(data []byte, dst []string) ([]string, int, error) {
	idx := bytes.IndexByte(data, '\n')
	if idx == -1 {
		return nil, 0, nil // Incomplete
	}

	lineEnd := idx
	if lineEnd > 0 && data[lineEnd-1] == '\r' {
		lineEnd--
	}

	line := data[:lineEnd]
	consumed := idx + 1

	fields := bytes.Fields(line)
	if len(fields) == 0 {
		return dst[:0], consumed, nil
	}

	args := dst[:0]
	if cap(args) < len(fields) {
		args = make([]string, 0, len(fields))
	}
	for _, f := range fields {
		args = append(args, string(f))
	}

	return args, consumed, nil
}
