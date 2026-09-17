package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	MaxBulkLength  = 512 * 1024 * 1024 // 512MB max bulk string
	MaxArrayLength = 1024 * 1024       // 1M elements max multibulk array to prevent OOM
)

// Reader provides high-throughput, low-allocation buffered reading of RESP commands
type Reader struct {
	reader *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		reader: bufio.NewReaderSize(r, 64*1024), // 64KB read buffer
	}
}

// Buffered returns the number of bytes currently buffered and waiting to be read
func (r *Reader) Buffered() int {
	return r.reader.Buffered()
}

// ReadValue reads the next RESP value or inline command
func (r *Reader) ReadValue() (Value, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch b {
	case SimpleStringPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return SimpleString(string(line)), nil

	case ErrorPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Error(string(line)), nil

	case IntegerPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, ErrInvalidSyntax
		}
		return Integer(n), nil

	case BulkStringPrefix:
		return r.readBulk()

	case ArrayPrefix:
		return r.readArray()

	case NullPrefix:
		// RESP3 null
		if _, err := r.readLine(); err != nil {
			return Value{}, err
		}
		return Null(), nil

	case BooleanPrefix:
		// RESP3 boolean
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		isTrue := len(line) > 0 && line[0] == 't'
		return Value{Type: BooleanPrefix, Bool: isTrue}, nil

	default:
		// Treat as inline command (e.g. "PING\r\n" or "SET foo bar\r\n")
		return r.readInline(b)
	}
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.reader.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	n := len(line)
	if n < 2 || line[n-2] != '\r' {
		// Could be just '\n' in telnet or pipe
		if n >= 1 && line[n-1] == '\n' {
			return line[:n-1], nil
		}
		return nil, ErrInvalidSyntax
	}
	return line[:n-2], nil
}

func (r *Reader) readBulk() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, ErrInvalidSyntax
	}

	if length == -1 {
		return Null(), nil
	}
	if length < -1 || length > MaxBulkLength {
		return Value{}, ErrBulkTooLarge
	}

	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		return Value{}, err
	}

	if buf[length] != '\r' || buf[length+1] != '\n' {
		return Value{}, ErrInvalidSyntax
	}

	return Value{
		Type: BulkStringPrefix,
		Bulk: buf[:length],
	}, nil
}

func (r *Reader) readArray() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, ErrInvalidSyntax
	}

	if length == -1 {
		return NullArray(), nil
	}
	if length < -1 {
		return Value{}, ErrInvalidSyntax
	}
	if length > MaxArrayLength {
		return Value{}, ErrMultibulkTooLarge
	}

	items := make([]Value, length)
	for i := int64(0); i < length; i++ {
		val, err := r.ReadValue()
		if err != nil {
			return Value{}, err
		}
		items[i] = val
	}

	return Value{
		Type:  ArrayPrefix,
		Array: items,
	}, nil
}

// readInline parses space-separated inline command (e.g. from telnet: "SET foo bar\r\n")
func (r *Reader) readInline(firstByte byte) (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	fullLine := append([]byte{firstByte}, line...)
	parts := parseCommandLine(string(fullLine))
	if len(parts) == 0 {
		return Array([]Value{}), nil
	}

	items := make([]Value, len(parts))
	for i, part := range parts {
		items[i] = BulkString(part)
	}

	return Array(items), nil
}

func parseCommandLine(cmd string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		if inQuotes {
			if c == quoteChar {
				inQuotes = false
			} else if c == '\\' && i+1 < len(cmd) {
				i++
				current.WriteByte(cmd[i])
			} else {
				current.WriteByte(c)
			}
		} else {
			if c == '"' || c == '\'' {
				inQuotes = true
				quoteChar = c
			} else if c == ' ' || c == '\t' {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
			} else {
				current.WriteByte(c)
			}
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

// ReadCommand parses a command array into arguments strings
func (r *Reader) ReadCommand() ([]string, error) {
	val, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	if val.Type != ArrayPrefix {
		if val.Type == BulkStringPrefix {
			return []string{string(val.Bulk)}, nil
		}
		return nil, fmt.Errorf("expected array command, got type %c", val.Type)
	}

	cmd := make([]string, len(val.Array))
	for i, item := range val.Array {
		if item.Type == BulkStringPrefix {
			cmd[i] = string(item.Bulk)
		} else if item.Type == SimpleStringPrefix {
			cmd[i] = item.Str
		} else if item.Type == IntegerPrefix {
			cmd[i] = strconv.FormatInt(item.Num, 10)
		} else {
			cmd[i] = item.String()
		}
	}

	return cmd, nil
}
