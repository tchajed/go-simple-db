package machine

import "encoding/binary"

// UInt64Get converts the first 8 bytes of p to a uint64.
//
// Requires p be at least 8 bytes long.
//
// Happens to decode in little-endian byte order, but this is only relevant as
// far as the relationship between UInt64Get and UInt64Put.
func UInt64Get(p []byte) uint64 {
	return binary.LittleEndian.Uint64(p)
}
