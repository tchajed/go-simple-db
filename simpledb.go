package simpleb

import "github.com/tchajed/go-simple-db/filesys"

var fs filesys.Filesys = filesys.Fs

// Init initializes the global filesystem layer.
func Init() {
	filesys.Init()
}

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

// CloseTable frees up the fd held by a table.
func CloseTable(t Table) {
	fs.Close(t.File)
}
