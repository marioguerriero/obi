package main

import (
		"flag"
	"github.com/golang/glog"
	"obi/utils"
	"obi/heartbeat"
		"obi/pooling"
	"time"
)

func main() {
	flag.Parse()
	pool := utils.NewConcurrentMap()

	// instantiate modules
	p := pooling.New(pool)
	hb := heartbeat.GetInstance(pool, 60, 30)
	hb.Start()
	// hb.Stop()

	// submit a job
	p.SubmitPySparkJob("obi-test", "gs://dhg-obi/cluster-script/word_count.py")
	glog.Flush()

	time.Sleep(time.Minute * 15)
}
