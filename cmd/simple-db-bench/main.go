package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/pprof"
)

type Config struct {
	DatabaseDir  string
	DatabaseSize int
	BenchFilter  *regexp.Regexp
	ListBenches  bool
}

func (conf Config) runBench(name string, par int, f func(b *bencher)) {
	if !conf.BenchFilter.MatchString(name) {
		return
	}
	if conf.ListBenches {
		fmt.Printf("%-25s\n", name)
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
	flag.BoolVar(&conf.ListBenches, "list", false,
		"list (matching) benchmarks without running them")
	filterString := flag.String("run", "",
		"regex to BenchFilter benchmarks (empty string means run all)")
	var kiters int
	flag.IntVar(&kiters, "kiters", 1000,
		"thousands of iterations to run")
	var par int
	flag.IntVar(&par, "par", 2,
		"number of concurrent threads for concurrent benchmarks")
	var cpuprofile = flag.String("cpuprofile", "",
		"write cpu profile to `file`")
	flag.Parse()

	if filterString == nil || *filterString == "" {
		conf.BenchFilter = regexp.MustCompile(".*")
	} else {
		var err error
		conf.BenchFilter, err = regexp.Compile(*filterString)
		if err != nil {
			log.Fatalf("invalid BenchFilter %s: %s\n", *filterString, err)
		}
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	conf.runBench("writes", 1, func(b *bencher) {
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Write(0))
		}
		b.Compact()
	})

	conf.runBench("write + compact", 1, func(b *bencher) {
		b.Fill()
		b.Reset()
		stopCompaction := startCompaction(b)
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Write(0))
		}
		b.finish()
		numCompactions := <-stopCompaction
		fmt.Printf("  finished %d compactions\n", numCompactions)
	})

	conf.runBench("rbuf reads", 1, func(b *bencher) {
		b.Fill()
		b.Reset()
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Read(0))
		}
	})
	conf.runBench("table reads", 1, func(b *bencher) {
		b.Fill()
		b.Compact()
		b.Compact()
		b.Reset()
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Read(0))
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
						b.finishOp(tid, b.Read(tid))
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
						b.finishOp(tid, b.Read(tid))
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
						b.finishOp(tid, b.Read(tid))
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
