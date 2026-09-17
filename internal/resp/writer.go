package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer provides high-throughput buffered writing of RESP output
type Writer struct {
	writer *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: bufio.NewWriterSize(w, 64*1024), // 64KB write buffer
	}
}

var (
	respOK        = []byte("+OK\r\n")
	respPONG      = []byte("+PONG\r\n")
	respNull      = []byte("$-1\r\n")
	respNullArray = []byte("*-1\r\n")
	respZeroInt   = []byte(":0\r\n")
	respOneInt    = []byte(":1\r\n")
)

func (w *Writer) Flush() error {
	return w.writer.Flush()
}

func (w *Writer) WriteSimpleString(s string) error {
	if s == "OK" {
		return w.WriteOK()
	}
	if s == "PONG" {
		return w.WritePong()
	}
	w.writer.WriteByte(SimpleStringPrefix)
	w.writer.WriteString(s)
	_, err := w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteOK() error {
	_, err := w.writer.Write(respOK)
	return err
}

func (w *Writer) WritePong() error {
	_, err := w.writer.Write(respPONG)
	return err
}

func (w *Writer) WriteError(msg string) error {
	w.writer.WriteByte(ErrorPrefix)
	w.writer.WriteString(msg)
	_, err := w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteInteger(n int64) error {
	if n == 0 {
		_, err := w.writer.Write(respZeroInt)
		return err
	}
	if n == 1 {
		_, err := w.writer.Write(respOneInt)
		return err
	}
	w.writer.WriteByte(IntegerPrefix)
	w.writer.WriteString(strconv.FormatInt(n, 10))
	_, err := w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteBulkString(s string) error {
	w.writer.WriteByte(BulkStringPrefix)
	w.writer.WriteString(strconv.Itoa(len(s)))
	w.writer.Write(CRLF)
	w.writer.WriteString(s)
	_, err := w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteBulkBytes(b []byte) error {
	if b == nil {
		return w.WriteNull()
	}
	w.writer.WriteByte(BulkStringPrefix)
	w.writer.WriteString(strconv.Itoa(len(b)))
	w.writer.Write(CRLF)
	w.writer.Write(b)
	_, err := w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteNull() error {
	_, err := w.writer.Write(respNull)
	return err
}

func (w *Writer) WriteNullArray() error {
	_, err := w.writer.Write(respNullArray)
	return err
}

func (w *Writer) WriteArrayHeader(length int) error {
	w.writer.WriteByte(ArrayPrefix)
	w.writer.WriteString(strconv.Itoa(length))
	_, err := w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteStringArray(items []string) error {
	if items == nil {
		return w.WriteNullArray()
	}
	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.WriteBulkString(item); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) WriteValue(v Value) error {
	switch v.Type {
	case SimpleStringPrefix:
		return w.WriteSimpleString(v.Str)
	case ErrorPrefix:
		return w.WriteError(v.Str)
	case IntegerPrefix:
		return w.WriteInteger(v.Num)
	case BulkStringPrefix:
		if v.Null {
			return w.WriteNull()
		}
		return w.WriteBulkBytes(v.Bulk)
	case ArrayPrefix:
		if v.Null {
			return w.WriteNullArray()
		}
		if err := w.WriteArrayHeader(len(v.Array)); err != nil {
			return err
		}
		for _, item := range v.Array {
			if err := w.WriteValue(item); err != nil {
				return err
			}
		}
		return nil
	default:
		return w.WriteBulkString(v.String())
	}
}

// SerializeValue serializes a Value into bytes (useful for AOF replication or snapshots)
func SerializeValue(v Value) []byte {
	var buf []byte
	switch v.Type {
	case SimpleStringPrefix:
		buf = append(buf, SimpleStringPrefix)
		buf = append(buf, []byte(v.Str)...)
		buf = append(buf, CRLF...)
	case ErrorPrefix:
		buf = append(buf, ErrorPrefix)
		buf = append(buf, []byte(v.Str)...)
		buf = append(buf, CRLF...)
	case IntegerPrefix:
		buf = append(buf, IntegerPrefix)
		buf = append(buf, []byte(strconv.FormatInt(v.Num, 10))...)
		buf = append(buf, CRLF...)
	case BulkStringPrefix:
		if v.Null {
			buf = append(buf, []byte("$-1\r\n")...)
		} else {
			buf = append(buf, BulkStringPrefix)
			buf = append(buf, []byte(strconv.Itoa(len(v.Bulk)))...)
			buf = append(buf, CRLF...)
			buf = append(buf, v.Bulk...)
			buf = append(buf, CRLF...)
		}
	case ArrayPrefix:
		if v.Null {
			buf = append(buf, []byte("*-1\r\n")...)
		} else {
			buf = append(buf, ArrayPrefix)
			buf = append(buf, []byte(strconv.Itoa(len(v.Array)))...)
			buf = append(buf, CRLF...)
			for _, item := range v.Array {
				buf = append(buf, SerializeValue(item)...)
			}
		}
	}
	return buf
}

// SerializeCommand serializes raw command string slices into RESP Array bytes
func SerializeCommand(args []string) []byte {
	var b []byte
	b = append(b, ArrayPrefix)
	b = append(b, []byte(strconv.Itoa(len(args)))...)
	b = append(b, CRLF...)
	for _, arg := range args {
		b = append(b, BulkStringPrefix)
		b = append(b, []byte(strconv.Itoa(len(arg)))...)
		b = append(b, CRLF...)
		b = append(b, []byte(arg)...)
		b = append(b, CRLF...)
	}
	return b
}
