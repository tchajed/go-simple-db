package simpledb

import "github.com/tchajed/goose/machine"
import "github.com/tchajed/goose/machine/filesys"

// Note that this code does not initialize the filesystem, because it happens
// outside of the Coq model (the lower-level layer is implicitly initialized)
//
// However, when this code runs of course something has to initialize the
// filesystem.
//
// TODO: this doesn't go through a pointer so initializing filesys.Fs later
// won't actually initialize this layer
var fs filesys.Filesys = filesys.Fs

// A Table provides access to an immutable copy of data on the filesystem, along
// with an index for fast random access.
type Table struct {
	Index map[uint64]uint64
	File  filesys.File
}

// CreateTable creates a new, empty table.
func CreateTable(p string) Table {
	index := make(map[uint64]uint64)
	f := fs.Create(p)
	fs.Close(f)
	f2 := fs.Open(p)
	return Table{Index: index, File: f2}
}

// Entry represents a (key, value) pair.
type Entry struct {
	Key   uint64
	Value []byte
}

// DecodeUInt64 is a Decoder(uint64)
//
// All decoders have the shape func(p []byte) (T, uint64)
//
// The uint64 represents the number of bytes consumed; if 0, then decoding
// failed, and the value of type T should be ignored.
func DecodeUInt64(p []byte) (uint64, uint64) {
	if len(p) < 8 {
		return 0, 0
	}
	n := machine.UInt64Get(p)
	return n, 8
}

// DecodeEntry is a Decoder(Entry)
func DecodeEntry(data []byte) (Entry, uint64) {
	key, l1 := DecodeUInt64(data)
	if l1 == 0 {
		return Entry{Key: 0, Value: nil}, 0
	}
	valueLen, l2 := DecodeUInt64(data[l1:])
	if l2 == 0 {
		return Entry{Key: 0, Value: nil}, 0
	}
	value := data[l1+l2 : l1+l2+valueLen]
	return Entry{Key: key, Value: value}, l1 + l2 + valueLen
}

type lazyFileBuf struct {
	offset uint64
	next   []byte
}

// readTableIndex parses a complete table on disk into a key->offset index
/*
func readTableIndex(f filesys.File, index map[uint64]uint64) {
	for buf := (lazyFileBuf{offset: 0, next: nil}); ; {
		e, l := DecodeEntry(buf.next)
		if l > 0 {
			index[e.Key] = 8 + buf.offset
			buf = lazyFileBuf{offset: buf.offset + 1, next: buf.next[l:]}
			continue
		} else {
			p := fs.ReadAt(f, buf.offset, 4096)
			if len(p) == 0 {
				break
			} else {
				buf = lazyFileBuf{
					offset: buf.offset,
					next:   append(buf.next, p...),
				}
				continue
			}
		}
	}
}
*/

// readTableIndex placeholder
func readTableIndex(f filesys.File, index map[uint64]uint64) {
}

// RecoverTable restores a table from disk on startup.
func RecoverTable(p string) Table {
	index := make(map[uint64]uint64)
	f := fs.Open(p)
	readTableIndex(f, index)
	return Table{Index: index, File: f}
}

// CloseTable frees up the fd held by a table.
func CloseTable(t Table) {
	fs.Close(t.File)
}
