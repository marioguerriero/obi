package main

import (
	"obi/pooling"
	"flag"
	"github.com/golang/glog"
)

func main() {
	flag.Parse()
	p := pooling.New()
	p.SubmitPySparkJob("obi-test", "gs://dhg-obi/cluster-script/word_count.py")
	glog.Flush()
}
