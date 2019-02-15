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
