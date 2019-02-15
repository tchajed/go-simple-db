package simpledb

import (
	"testing"

	"github.com/tchajed/goose/machine/filesys"
)
import . "gopkg.in/check.v1"

func Test(t *testing.T) { TestingT(t) }

type SimpleDbSuite struct{}

var _ = Suite(&SimpleDbSuite{})

func (s *SimpleDbSuite) SetUpTest(c *C) {
	filesys.Fs = filesys.MemFs()
}

func readFile(p string) (data []byte) {
	f := filesys.Open(p)
	defer filesys.Close(f)
	for off := uint64(0); ; off += 4096 {
		buf := filesys.ReadAt(f, off, 4096)
		data = append(data, buf...)
		if len(buf) < 4096 {
			return
		}
	}
}

func (s *SimpleDbSuite) TestBufFile(c *C) {
	f := newBuf(filesys.Create("test"))
	bufAppend(f, []byte("hello "))
	bufAppend(f, []byte("world"))
	bufFlush(f)
	bufAppend(f, []byte("!"))
	bufClose(f)
	c.Check(readFile("test"), DeepEquals, []byte("hello world!"))
}

type readValue struct {
	value   []byte
	present bool
}

func tableRead(t Table, k uint64) readValue {
	v, ok := TableRead(t, k)
	return readValue{value: v, present: ok}
}

var missing = readValue{value: nil, present: false}

func present(v string) readValue {
	return readValue{
		value:   []byte(v),
		present: true,
	}
}

func (s *SimpleDbSuite) TestTableWriter(c *C) {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	t := tableWriterClose(w)
	c.Check(tableRead(t, 1), DeepEquals, present("v1"))
	c.Check(tableRead(t, 2), DeepEquals, present("v two"))
	c.Check(tableRead(t, 10), DeepEquals, present("value ten"))
}

func (s *SimpleDbSuite) TestTableRecovery(c *C) {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	tmp := tableWriterClose(w)
	CloseTable(tmp)

	t := RecoverTable("table")
	c.Check(tableRead(t, 1), DeepEquals, present("v1"))
	c.Check(tableRead(t, 2), DeepEquals, present("v two"))
	c.Check(tableRead(t, 10), DeepEquals, present("value ten"))
}

func dbRead(db Database, k uint64) readValue {
	v, ok := Read(db, k)
	return readValue{value: v, present: ok}
}

func (s *SimpleDbSuite) TestReadWrite(c *C) {
	db := NewDb()
	c.Check(dbRead(db, 1), DeepEquals, missing)
	Write(db, 1, []byte("v1"))
	Write(db, 2, []byte("value 2"))
	c.Check(dbRead(db, 1), DeepEquals, present("v1"))
	c.Check(dbRead(db, 2), DeepEquals, present("value 2"))
}
