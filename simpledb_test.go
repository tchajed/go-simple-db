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

func (s *SimpleDbSuite) TestTableWriter(c *C) {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	t := tableWriterClose(w)
	c.Check(TableRead(t, 1), DeepEquals, []byte("v1"))
	c.Check(TableRead(t, 2), DeepEquals, []byte("v two"))
	c.Check(TableRead(t, 10), DeepEquals, []byte("value ten"))
}

func (s *SimpleDbSuite) TestTableRecovery(c *C) {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	tmp := tableWriterClose(w)
	CloseTable(tmp)

	t := RecoverTable("table")
	c.Check(TableRead(t, 1), DeepEquals, []byte("v1"))
	c.Check(TableRead(t, 2), DeepEquals, []byte("v two"))
	c.Check(TableRead(t, 10), DeepEquals, []byte("value ten"))
}
