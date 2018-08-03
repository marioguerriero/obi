package main

import (
			"github.com/golang/glog"
	"obi/utils"
	"obi/heartbeat"
		"obi/pooling"
	"time"
		"os"
		"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetOutput(os.Stdout)
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
