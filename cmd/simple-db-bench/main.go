package main

import (
	"flag"
	"fmt"
)

func (conf Config) runBench(name string, par int, f func(b *bencher)) {
	b := newBench(conf, name, par)
	f(&b)
	b.finish()
}

func main() {
	var conf Config
	flag.StringVar(&conf.DatabaseDir, "dir", "bench.dir",
		"directory to store database in")
	flag.IntVar(&conf.DatabaseSize, "size", 1000,
		"size of database")
	var kiters int
	flag.IntVar(&kiters, "kiters", 1000,
		"thousands of iterations to run")
	var par int
	flag.IntVar(&par, "par", 2,
		"number of concurrent threads for concurrent benchmarks")
	flag.Parse()

	conf.runBench("writes", 1, func(b *bencher) {
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Write())
		}
		b.Compact()
	})

	conf.runBench("wbuf reads", 1, func(b *bencher) {
		for i := 0; i < 1000*kiters; i++ {
			b.finishOp(0, b.Write())
		}
		b.Compact()
	})
	conf.runBench("wbuf reads", 1, func(b *bencher) {
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
}
