package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/tchajed/go-simple-db"
	"github.com/tchajed/goose/machine/filesys"
)

type gen struct {
	rand    *rand.Rand
	maxKeys int
}

func newGen(maxKeys int) gen {
	return gen{
		rand:    rand.New(rand.NewSource(0)),
		maxKeys: maxKeys,
	}
}

func (g gen) RandomKey() uint64 {
	n := g.rand.Int63n(int64(g.maxKeys))
	return uint64(n)
}

func (g gen) Value() []byte {
	return make([]byte, 100)
}

type stats struct {
	ops   []int
	bytes []int
	start time.Time
	end   *time.Time
}

func newStats(numThreads int) stats {
	return stats{
		ops:   make([]int, numThreads),
		bytes: make([]int, numThreads),
		start: time.Now(),
		end:   nil,
	}
}

func (s *stats) finishOp(tid int, bytes int) {
	s.ops[tid]++
	s.bytes[tid] += bytes
}

func (s *stats) done() {
	if s.end != nil {
		panic("stats object marked done multiple times")
	}
	t := time.Now()
	s.end = &t
}

func (s stats) Par() int {
	return len(s.ops)
}

func (s stats) TotalOps() int {
	total := 0
	for _, ops := range s.ops {
		total += ops
	}
	return total
}

func (s stats) TotalBytes() int {
	total := 0
	for _, bytes := range s.bytes {
		total += bytes
	}
	return total
}

func (s stats) seconds() float64 {
	return s.end.Sub(s.start).Seconds()
}

func (s stats) MicrosPerOp() float64 {
	return (s.seconds() * 1e6) / float64(s.TotalOps())
}

func (s stats) MegabytesPerSec() float64 {
	mb := float64(s.TotalBytes()) / (1024 * 1024)
	return mb / s.seconds()
}

func (s stats) formatStats() string {
	if s.TotalBytes() == 0 {
		if s.TotalOps() == 1 {
			return fmt.Sprintf("%7.3f micros", s.MicrosPerOp())
		}
		return fmt.Sprintf("%7.3f micros/op", s.MicrosPerOp())
	}
	return fmt.Sprintf("%7.3f micros/op; %6.1f MB/s",
		s.MicrosPerOp(),
		s.MegabytesPerSec())
}

func prepareDb(dir string) simpledb.Database {
	err := os.Mkdir(dir, 0744)
	if os.IsExist(err) {
		_ = os.RemoveAll(dir)
		err = os.Mkdir(dir, 0744)
	}
	if err != nil {
		panic(err)
	}
	filesys.Fs = filesys.NewDirFs(dir)
	return simpledb.NewDb()
}

func shutdownDb(db simpledb.Database, dir string) {
	simpledb.Shutdown(db)
	err := os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}
}

type Config struct {
	DatabaseDir  string
	DatabaseSize int
}

type bencher struct {
	name string
	conf Config
	stats
	gen
	db simpledb.Database
}

func newBench(conf Config, name string, par int) bencher {
	db := prepareDb(conf.DatabaseDir)
	gen := newGen(conf.DatabaseSize)
	return bencher{
		name:  name,
		conf:  conf,
		stats: newStats(par),
		gen:   gen,
		db:    db,
	}
}

func (b *bencher) Reset() {
	b.stats = newStats(b.stats.Par())
}

func (b *bencher) finish() {
	b.stats.done()
	fmt.Printf("%-20s : %s\n", b.name, b.stats.formatStats())
	shutdownDb(b.db, b.conf.DatabaseDir)
}

// Read a random key. Returns the bytes of data read.
func (b *bencher) Read() int {
	v, ok := simpledb.Read(b.db, b.RandomKey())
	if !ok {
		return 0
	}
	return len(v)
}

func (b *bencher) writeKey(k uint64) int {
	v := b.Value()
	simpledb.Write(b.db, k, v)
	return len(v)
}

// Write a random key. Returns the number of bytes written.
func (b *bencher) Write() int {
	return b.writeKey(b.RandomKey())
}

func (b *bencher) Fill() {
	for k := 0; k < b.maxKeys; k++ {
		b.writeKey(uint64(k))
	}
}

func (b *bencher) Compact() {
	simpledb.Compact(b.db)
}
