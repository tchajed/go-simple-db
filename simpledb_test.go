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
	filesys.Fs = filesys.NewMemFs()
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

func (s *SimpleDbSuite) TestEntryEncoding(c *C) {
	e := Entry{Key: 3, Value: []byte("value")}
	var buf []byte
	buf = EncodeUInt64(e.Key, buf)
	buf = EncodeSlice(e.Value, buf)

	decoded, l := DecodeEntry(buf)
	c.Assert(l, Equals, uint64(len(buf)))
	c.Check(decoded, DeepEquals, e)
}

func (s *SimpleDbSuite) TestEntryEncodingShort(c *C) {
	e := Entry{Key: 3, Value: []byte("value")}
	var buf []byte
	buf = EncodeUInt64(e.Key, buf)
	buf = EncodeSlice(e.Value, buf)

	_, l := DecodeEntry(buf[:len(buf)-1])
	c.Assert(l, Equals, uint64(0))
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

type maybeValue struct {
	value   []byte
	present bool
}

func tblRead(t Table, k uint64) maybeValue {
	v, ok := tableRead(t, k)
	return maybeValue{value: v, present: ok}
}

var missing = maybeValue{value: nil, present: false}

func bytesPresent(data []byte) maybeValue {
	return maybeValue{
		value:   data,
		present: true,
	}
}

func present(v string) maybeValue {
	return bytesPresent([]byte(v))
}

func (s *SimpleDbSuite) TestTableWriter(c *C) {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	t := tableWriterClose(w)
	c.Check(tblRead(t, 1), DeepEquals, present("v1"))
	c.Check(tblRead(t, 2), DeepEquals, present("v two"))
	c.Check(tblRead(t, 10), DeepEquals, present("value ten"))
}

func (s *SimpleDbSuite) TestTableWriterLargeValue(c *C) {
	w := newTableWriter("table")
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	tablePut(w, 1, data)
	t := tableWriterClose(w)
	c.Check(tblRead(t, 1), DeepEquals, bytesPresent(data))
}

func (s *SimpleDbSuite) TestTableRecovery(c *C) {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	tmp := tableWriterClose(w)
	CloseTable(tmp)

	t := RecoverTable("table")
	c.Check(tblRead(t, 1), DeepEquals, present("v1"))
	c.Check(tblRead(t, 2), DeepEquals, present("v two"))
	c.Check(tblRead(t, 10), DeepEquals, present("value ten"))
}

func dbRead(db Database, k uint64) maybeValue {
	v, ok := Read(db, k)
	return maybeValue{value: v, present: ok}
}

func (s *SimpleDbSuite) TestReadWrite(c *C) {
	db := NewDb()
	c.Check(dbRead(db, 1), DeepEquals, missing)
	Write(db, 1, []byte("v1"))
	Write(db, 2, []byte("value 2"))
	c.Check(dbRead(db, 1), DeepEquals, present("v1"))
	c.Check(dbRead(db, 2), DeepEquals, present("value 2"))
}

func (s *SimpleDbSuite) TestCompact(c *C) {
	db := NewDb()
	c.Check(dbRead(db, 1), DeepEquals, missing)
	Write(db, 1, []byte("v1"))
	Compact(db)
	Compact(db)
	Write(db, 2, []byte("value 2"))
	c.Check(dbRead(db, 1), DeepEquals, present("v1"))
	c.Check(dbRead(db, 2), DeepEquals, present("value 2"))
}

func (s *SimpleDbSuite) TestRecover(c *C) {
	db := NewDb()
	c.Check(dbRead(db, 1), DeepEquals, missing)
	Write(db, 1, []byte("v1"))
	Compact(db)
	Compact(db)
	Write(db, 2, []byte("value 2"))
	Shutdown(db)
	db = Recover()
	c.Check(dbRead(db, 1), DeepEquals, present("v1"))
	c.Check(dbRead(db, 2), DeepEquals, missing)
}

func (s *SimpleDbSuite) TestClose(c *C) {
	db := NewDb()
	c.Check(dbRead(db, 1), DeepEquals, missing)
	Write(db, 1, []byte("v1"))
	Compact(db)
	Compact(db)
	Write(db, 2, []byte("value 2"))
	Close(db)
	db = Recover()
	c.Check(dbRead(db, 1), DeepEquals, present("v1"))
	c.Check(dbRead(db, 2), DeepEquals, present("value 2"))
}

func (s *SimpleDbSuite) TestReadBuffer(c *C) {
	db := NewDb()
	Write(db, 1, []byte("v1"))
	Compact(db)
	c.Check(dbRead(db, 1), DeepEquals, present("v1"))
}

func (s *SimpleDbSuite) TestReadLargeValue(c *C) {
	db := NewDb()
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	Write(db, 1, data)
	Compact(db)
	Compact(db)
	c.Check(dbRead(db, 1), DeepEquals, bytesPresent(data))
}

func (s *SimpleDbSuite) TestRecoverLargeValue(c *C) {
	db := NewDb()
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	Write(db, 1, data)
	Close(db)
	db = Recover()
	c.Check(dbRead(db, 1), DeepEquals, bytesPresent(data))
}
