package main

import (
		"flag"
	"github.com/golang/glog"
	"obi/utils"
	"obi/heartbeat"
	"time"
	"obi/pooling"
)

func main() {
	flag.Parse()
	pool := utils.NewConcurrentMap()

	// instantiate modules
	p := pooling.New(pool)
	hb := heartbeat.GetInstance(pool, 60, 30)
	hb.Start()
	time.Sleep(30 * time.Second)
	hb.Stop()
	time.Sleep(30 * time.Second)

	// submit a job
	p.SubmitPySparkJob("obi-test", "gs://dhg-obi/cluster-script/word_count.py")
	glog.Flush()

}
