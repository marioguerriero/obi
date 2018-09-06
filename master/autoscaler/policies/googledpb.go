package policies

import (
	"obi/master/model"
	"fmt"
	"github.com/sirupsen/logrus"
	"obi/master/utils"
)

// WorkloadPolicy contains all useful state-variable to apply the policy
type GooglePolicy struct {
}

func NewGoogle() *GooglePolicy {
	return &GooglePolicy{}
}

func (p *GooglePolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var count int32
	var memoryUsage int32
	workerMemory := 15000.0

	logrus.Info("Applying time-based policy")
	for obj := range metricsWindow.Iter() {
		if obj.Value == nil {
			continue
		}

		hb := obj.Value.(model.Metrics)
		memoryUsage += hb.PendingMemory - hb.AvailableMemory
		count++
	}

	if count > 0 {
		workers := float64(memoryUsage / count) / workerMemory
		fmt.Printf("Exact workers: %f\n", workers)
		return int32(workers)
	}
	return 0
}
