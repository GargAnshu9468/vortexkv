package reactor

// 32-bit representations of ASCII commands (lowercased via bitwise OR with 0x20).
// Little-endian byte layout: byte0 | (byte1 << 8) | (byte2 << 16) | (byte3 << 24)
const (
	// 3-letter commands
	CmdGet = uint32('g') | (uint32('e') << 8) | (uint32('t') << 16)
	CmdSet = uint32('s') | (uint32('e') << 8) | (uint32('t') << 16)
	CmdDel = uint32('d') | (uint32('e') << 8) | (uint32('l') << 16)

	// 4-letter commands
	CmdPing = uint32('p') | (uint32('i') << 8) | (uint32('n') << 16) | (uint32('g') << 24)
	CmdQuit = uint32('q') | (uint32('u') << 8) | (uint32('i') << 16) | (uint32('t') << 24)
	CmdIncr = uint32('i') | (uint32('n') << 8) | (uint32('c') << 16) | (uint32('r') << 24)
	CmdDecr = uint32('d') | (uint32('e') << 8) | (uint32('c') << 16) | (uint32('r') << 24)
	CmdAuth = uint32('a') | (uint32('u') << 8) | (uint32('t') << 16) | (uint32('h') << 24)
	CmdMget = uint32('m') | (uint32('g') << 8) | (uint32('e') << 16) | (uint32('t') << 24)
	CmdMset = uint32('m') | (uint32('s') << 8) | (uint32('e') << 16) | (uint32('t') << 24)
	CmdHget = uint32('h') | (uint32('g') << 8) | (uint32('e') << 16) | (uint32('t') << 24)
	CmdHset = uint32('h') | (uint32('s') << 8) | (uint32('e') << 16) | (uint32('t') << 24)
	CmdInfo = uint32('i') | (uint32('n') << 8) | (uint32('f') << 16) | (uint32('o') << 24)
	CmdEcho = uint32('e') | (uint32('c') << 8) | (uint32('h') << 16) | (uint32('o') << 24)
	CmdType = uint32('t') | (uint32('y') << 8) | (uint32('p') << 16) | (uint32('e') << 24)
)

// AsCmd3 extracts a 3-letter command as uint32 case-insensitively with zero heap allocations.
func AsCmd3(s string) uint32 {
	if len(s) != 3 {
		return 0
	}
	return uint32(s[0]|0x20) | (uint32(s[1]|0x20) << 8) | (uint32(s[2]|0x20) << 16)
}

// AsCmd4 extracts a 4-letter command as uint32 case-insensitively with zero heap allocations.
func AsCmd4(s string) uint32 {
	if len(s) != 4 {
		return 0
	}
	return uint32(s[0]|0x20) | (uint32(s[1]|0x20) << 8) | (uint32(s[2]|0x20) << 16) | (uint32(s[3]|0x20) << 24)
}
