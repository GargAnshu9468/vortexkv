package reactor

import (
	"strconv"

	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

var (
	respOK   = []byte("+OK\r\n")
	respPONG = []byte("+PONG\r\n")
	respNull = []byte("$-1\r\n")
	respZero = []byte(":0\r\n")
	respOne  = []byte(":1\r\n")
)

// AppendValue serializes a resp.Value directly into dst slice with zero allocations.
func AppendValue(dst []byte, v resp.Value) []byte {
	if v.Null {
		if v.Type == resp.ArrayPrefix {
			return append(dst, '*', '-', '1', '\r', '\n')
		}
		return append(dst, respNull...)
	}

	switch v.Type {
	case resp.SimpleStringPrefix:
		if v.Str == "OK" {
			return append(dst, respOK...)
		}
		if v.Str == "PONG" {
			return append(dst, respPONG...)
		}
		dst = append(dst, '+')
		dst = append(dst, v.Str...)
		return append(dst, '\r', '\n')

	case resp.ErrorPrefix:
		dst = append(dst, '-')
		dst = append(dst, v.Str...)
		return append(dst, '\r', '\n')

	case resp.IntegerPrefix:
		if v.Num == 0 {
			return append(dst, respZero...)
		}
		if v.Num == 1 {
			return append(dst, respOne...)
		}
		dst = append(dst, ':')
		dst = strconv.AppendInt(dst, v.Num, 10)
		return append(dst, '\r', '\n')

	case resp.BulkStringPrefix:
		if len(v.Bulk) > 0 {
			dst = append(dst, '$')
			dst = strconv.AppendInt(dst, int64(len(v.Bulk)), 10)
			dst = append(dst, '\r', '\n')
			dst = append(dst, v.Bulk...)
			return append(dst, '\r', '\n')
		}
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(v.Str)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, v.Str...)
		return append(dst, '\r', '\n')

	case resp.ArrayPrefix:
		dst = append(dst, '*')
		dst = strconv.AppendInt(dst, int64(len(v.Array)), 10)
		dst = append(dst, '\r', '\n')
		for i := range v.Array {
			dst = AppendValue(dst, v.Array[i])
		}
		return dst

	default:
		// Fallback for empty or unknown
		if v.Str != "" {
			dst = append(dst, '+')
			dst = append(dst, v.Str...)
			return append(dst, '\r', '\n')
		}
		return append(dst, respOK...)
	}
}
