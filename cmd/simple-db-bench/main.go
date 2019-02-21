package main

import (
	"flag"
)

func main() {
	var conf Config
	flag.StringVar(&conf.DatabaseDir, "dir", "bench.dir",
		"directory to store database in")
	flag.IntVar(&conf.DatabaseSize, "size", 1000,
		"size of database")
	var kiters int
	flag.IntVar(&kiters, "kiters", 1000,
		"thousands of iterations to run")
	flag.Parse()

	var b bencher
	b = newBench(conf, "writes")
	for i := 0; i < 1000*kiters; i++ {
		b.finishOp(b.Write())
	}
	b.Compact()
	b.finish()

	b = newBench(conf, "wbuf reads")
	b.Fill()
	b.Reset()
	for i := 0; i < 1000*kiters; i++ {
		b.finishOp(b.Read())
	}
	b.finish()

	b = newBench(conf, "table reads")
	b.Fill()
	b.Compact()
	b.Compact()
	b.Reset()
	for i := 0; i < 1000*kiters; i++ {
		b.finishOp(b.Read())
	}
	b.finish()
}
