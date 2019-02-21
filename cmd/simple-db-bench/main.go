package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
)

type Config struct {
	DatabaseDir  string
	DatabaseSize int
	BenchFilter  string
	filter       *regexp.Regexp
}

func (conf Config) runBench(name string, par int, f func(b *bencher)) {
	if !conf.filter.MatchString(name) {
		return
	}
	b := newBench(conf, name, par)
	f(&b)
	if !b.IsFinished() {
		b.finish()
	}
	b.stop()
}

// startCompaction starts running compactions continuously
//
// startCompaction returns a channel. To stop compaction,
// read from this channel; after the current compaction finishes,
// the read will return the number of compactions completed and no more will
// run.
func startCompaction(b *bencher) (done chan int) {
	done = make(chan int)
	numCompactions := 0
	go func() {
		for {
			select {
			case done <- numCompactions:
				return
			default:
				b.Compact()
				numCompactions++
			}
		}
	}()
	return done
}

func main() {
	var conf Config
	flag.StringVar(&conf.DatabaseDir, "dir", "bench.dir",
		"directory to store database in")
	flag.IntVar(&conf.DatabaseSize, "size", 10000,
		"size of database")
	flag.StringVar(&conf.BenchFilter, "run", "",
		"regex to filter benchmarks (empty string means run all)")
	var kiters int
	flag.IntVar(&kiters, "kiters", 1000,
		"thousands of iterations to run")
	var par int
	flag.IntVar(&par, "par", 2,
		"number of concurrent threads for concurrent benchmarks")
	flag.Parse()

	if conf.BenchFilter == "" {
		conf.filter = regexp.MustCompile(".*")
	} else {
		filter, err := regexp.Compile(conf.BenchFilter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid filter %s: %s\n",
				conf.BenchFilter, err)
			os.Exit(1)
		}
		conf.filter = filter
	}

	conf.runBench("writes", 1, func(b *bencher) {
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Write())
		}
		b.Compact()
	})

	conf.runBench("write + compact", 1, func(b *bencher) {
		b.Fill()
		b.Reset()
		stopCompaction := startCompaction(b)
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Write())
		}
		b.finish()
		numCompactions := <-stopCompaction
		fmt.Printf("  finished %d compactions\n", numCompactions)
	})

	conf.runBench("rbuf reads", 1, func(b *bencher) {
		b.Fill()
		b.Reset()
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Read())
		}
	})
	conf.runBench("table reads", 1, func(b *bencher) {
		b.Fill()
		b.Compact()
		b.Compact()
		b.Reset()
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Read())
		}
	})

	conf.runBench(fmt.Sprintf("table reads (par=%d)", par),
		par,
		func(b *bencher) {
			b.Fill()
			b.Compact()
			b.Compact()
			b.Reset()
			done := make(chan bool)
			for tid := 0; tid < par; tid++ {
				go func(tid int) {
					for i := 0; i < 1000*kiters; i++ {
						b.finishOp(tid, b.Read())
					}
					done <- true
				}(tid)
			}
			for tid := 0; tid < par; tid++ {
				<-done
			}
		})

	conf.runBench(fmt.Sprintf("rbuf reads (par=%d)", par),
		par,
		func(b *bencher) {
			b.Fill()
			b.Compact()
			b.Reset()
			done := make(chan bool)
			for tid := 0; tid < par; tid++ {
				go func(tid int) {
					for i := 0; i < 1000*kiters; i++ {
						b.finishOp(tid, b.Read())
					}
					done <- true
				}(tid)
			}
			for tid := 0; tid < par; tid++ {
				<-done
			}
		})

	conf.runBench(fmt.Sprintf("read par=%d + compact", par),
		par,
		func(b *bencher) {
			b.Fill()
			b.Compact()
			b.Reset()
			stopCompaction := startCompaction(b)
			done := make(chan bool)
			for tid := 0; tid < par; tid++ {
				go func(tid int) {
					for i := 0; i < 1000*kiters; i++ {
						b.finishOp(tid, b.Read())
					}
					done <- true
				}(tid)
			}
			for tid := 0; tid < par; tid++ {
				<-done
			}
			b.finish()
			numCompactions := <-stopCompaction
			fmt.Printf("  finished %d compactions\n", numCompactions)
		})
}
