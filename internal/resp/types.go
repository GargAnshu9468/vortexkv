package resp

import (
	"errors"
	"fmt"
	"strconv"
)

// RESP Protocol byte prefixes
const (
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	IntegerPrefix      = ':'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
	NullPrefix         = '_'
	BooleanPrefix      = '#'
	DoublePrefix       = ','
	BigNumberPrefix    = '('
	BulkErrorPrefix    = '!'
	VerbatimPrefix     = '='
	MapPrefix          = '%'
	SetPrefix          = '~'
)

var (
	CRLF = []byte("\r\n")

	ErrInvalidSyntax     = errors.New("resp: invalid protocol syntax")
	ErrUnexpectedEOF     = errors.New("resp: unexpected EOF")
	ErrBulkTooLarge      = errors.New("resp: bulk string size exceeds limit")
	ErrMultibulkTooLarge = errors.New("resp: multibulk length exceeds limit")
)

// Value represents any RESP value
type Value struct {
	Type  byte
	Str   string
	Num   int64
	Bulk  []byte
	Array []Value
	Bool  bool
	Null  bool
}

func SimpleString(s string) Value {
	return Value{Type: SimpleStringPrefix, Str: s}
}

func Error(msg string) Value {
	return Value{Type: ErrorPrefix, Str: msg}
}

func Integer(n int64) Value {
	return Value{Type: IntegerPrefix, Num: n}
}

func BulkString(s string) Value {
	return Value{Type: BulkStringPrefix, Bulk: []byte(s)}
}

func BulkBytes(b []byte) Value {
	return Value{Type: BulkStringPrefix, Bulk: b}
}

func Null() Value {
	return Value{Type: BulkStringPrefix, Null: true}
}

func NullArray() Value {
	return Value{Type: ArrayPrefix, Null: true}
}

func Array(items []Value) Value {
	return Value{Type: ArrayPrefix, Array: items}
}

func (v Value) String() string {
	switch v.Type {
	case SimpleStringPrefix, ErrorPrefix:
		return v.Str
	case IntegerPrefix:
		return strconv.FormatInt(v.Num, 10)
	case BulkStringPrefix:
		if v.Null {
			return "(nil)"
		}
		return string(v.Bulk)
	case ArrayPrefix:
		if v.Null {
			return "(nil)"
		}
		return fmt.Sprintf("[%d items]", len(v.Array))
	default:
		return ""
	}
}
