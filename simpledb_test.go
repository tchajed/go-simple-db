package simpledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/tchajed/goose/machine/filesys"
)

func readFile(p string) (data []byte) {
	f := filesys.Open("db", p)
	defer filesys.Close(f)
	for off := uint64(0); ; off += 4096 {
		buf := filesys.ReadAt(f, off, 4096)
		data = append(data, buf...)
		if len(buf) < 4096 {
			return
		}
	}
}

func TestEntryEncoding(t *testing.T) {
	assert := assert.New(t)
	e := Entry{Key: 3, Value: []byte("value")}
	var buf []byte
	buf = EncodeUInt64(e.Key, buf)
	buf = EncodeSlice(e.Value, buf)

	decoded, l := DecodeEntry(buf)
	assert.Equal(uint64(len(buf)), l)
	assert.Equal(e, decoded)
}

func TestEntryEncodingShort(t *testing.T) {
	e := Entry{Key: 3, Value: []byte("value")}
	var buf []byte
	buf = EncodeUInt64(e.Key, buf)
	buf = EncodeSlice(e.Value, buf)

	_, l := DecodeEntry(buf[:len(buf)-1])
	assert.Equal(t, uint64(0), l)
}

type SimpleDbSuite struct {
	suite.Suite
}

func TestSimpleDbSuite(t *testing.T) {
	suite.Run(t, new(SimpleDbSuite))
}

func (suite *SimpleDbSuite) SetupTest() {
	filesys.Fs = filesys.NewMemFs()
	filesys.Fs.Mkdir("db")
}

func (suite *SimpleDbSuite) TestBufFile() {
	testFile, _ := filesys.Create("db", "test")
	f := newBuf(testFile)
	bufAppend(f, []byte("hello "))
	bufAppend(f, []byte("world"))
	bufFlush(f)
	bufAppend(f, []byte("!"))
	bufClose(f)
	suite.Equal([]byte("hello world!"), readFile("test"))
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

func (suite *SimpleDbSuite) TestTableWriter() {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	t := tableWriterClose(w)
	suite.Equal(present("v1"), tblRead(t, 1))
	suite.Equal(present("v two"), tblRead(t, 2))
	suite.Equal(present("value ten"), tblRead(t, 10))
}

func (suite *SimpleDbSuite) TestTableWriterLargeValue() {
	w := newTableWriter("table")
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	tablePut(w, 1, data)
	t := tableWriterClose(w)
	suite.Equal(bytesPresent(data), tblRead(t, 1))
}

func (suite *SimpleDbSuite) TestTableRecovery() {
	w := newTableWriter("table")
	tablePut(w, 1, []byte("v1"))
	tablePut(w, 10, []byte("value ten"))
	tablePut(w, 2, []byte("v two"))
	tmp := tableWriterClose(w)
	CloseTable(tmp)

	tbl := RecoverTable("table")
	suite.Equal(present("v1"), tblRead(tbl, 1))
	suite.Equal(present("v two"), tblRead(tbl, 2))
	suite.Equal(present("value ten"), tblRead(tbl, 10))
}

func dbRead(db Database, k uint64) maybeValue {
	v, ok := Read(db, k)
	return maybeValue{value: v, present: ok}
}

func (suite *SimpleDbSuite) TestReadWrite() {
	db := NewDb()
	suite.Equal(missing, dbRead(db, 1))
	Write(db, 1, []byte("v1"))
	Write(db, 2, []byte("value 2"))
	suite.Equal(present("v1"), dbRead(db, 1))
	suite.Equal(present("value 2"), dbRead(db, 2))
}

func (suite *SimpleDbSuite) TestCompact() {
	db := NewDb()
	suite.Equal(missing, dbRead(db, 1))
	Write(db, 1, []byte("v1"))
	Compact(db)
	Compact(db)
	Write(db, 2, []byte("value 2"))
	suite.Equal(present("v1"), dbRead(db, 1))
	suite.Equal(present("value 2"), dbRead(db, 2))
}

func (suite *SimpleDbSuite) TestRecover() {
	db := NewDb()
	suite.Equal(missing, dbRead(db, 1))
	Write(db, 1, []byte("v1"))
	Compact(db)
	Compact(db)
	Write(db, 2, []byte("value 2"))
	Shutdown(db)
	db = Recover()
	suite.Equal(present("v1"), dbRead(db, 1))
	suite.Equal(missing, dbRead(db, 2))
}

func (suite *SimpleDbSuite) TestClose() {
	db := NewDb()
	suite.Equal(missing, dbRead(db, 1))
	Write(db, 1, []byte("v1"))
	Compact(db)
	Compact(db)
	Write(db, 2, []byte("value 2"))
	Close(db)
	db = Recover()
	suite.Equal(present("v1"), dbRead(db, 1))
	suite.Equal(present("value 2"), dbRead(db, 2))
}

func (suite *SimpleDbSuite) TestReadBuffer() {
	db := NewDb()
	Write(db, 1, []byte("v1"))
	Compact(db)
	suite.Equal(present("v1"), dbRead(db, 1))
}

func (suite *SimpleDbSuite) TestReadLargeValue() {
	db := NewDb()
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	Write(db, 1, data)
	Compact(db)
	Compact(db)
	suite.Equal(bytesPresent(data), dbRead(db, 1))
}

func (suite *SimpleDbSuite) TestRecoverLargeValue() {
	db := NewDb()
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	Write(db, 1, data)
	Close(db)
	db = Recover()
	suite.Equal(bytesPresent(data), dbRead(db, 1))
}
