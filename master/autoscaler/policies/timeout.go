package policies

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"obi/master/model"
	"obi/master/predictor"
	"obi/master/utils"
)

const TimeoutScalingStep = 1
const TimeoutScalingThreshold = 40

type TimeoutAutoscaler struct {
	scalingFactor int32
	record        *predictor.AutoscalerData
}

func NewTimeout() *TimeoutAutoscaler {
	return &TimeoutAutoscaler{}
}

// Workload scales the cluster when the resource utilization is too high
func (a* TimeoutAutoscaler) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.Metrics
	var throughput float32
	var pendingGrowthRate float32
	var count int8
	var performance float32

	logrus.Info("Applying workload-based policy")
	for obj := range metricsWindow.Iter() {
		if obj.Value == nil {
			continue
		}

		hb := obj.Value.(model.Metrics)

		if previousMetrics != (model.Metrics{}) {
			fmt.Printf("Allocated containers: %d\n", hb.TotalContainersAllocated)
			fmt.Printf("Released containers: %d\n", hb.TotalContainersReleased)
			fmt.Printf("Released containers before: %d\n", previousMetrics.TotalContainersReleased)
			throughput += float32(hb.TotalContainersReleased - previousMetrics.TotalContainersReleased)

			if hb.PendingContainers > 0 {
				fmt.Printf("Pending containers: %d\n", hb.PendingContainers)
				memoryContainer := hb.PendingMemory / hb.PendingContainers
				containersWillConsumed := hb.AvailableMemory / memoryContainer
				pendingGrowth := float32(hb.PendingContainers - containersWillConsumed - previousMetrics.PendingContainers)
				if pendingGrowth > 0 {
					pendingGrowthRate += pendingGrowth
				}
			}

			count++
		}
		previousMetrics = hb
	}

	if count > 0 {
		throughput /= float32(count)
		pendingGrowthRate /= float32(count)

		performance = throughput - pendingGrowthRate // I want to maximize this

		fmt.Printf("Throughput: %f\n", throughput)
		fmt.Printf("Pending rate: %f\n", pendingGrowthRate)

		// Scale up one at each time interval until we reach a threshold
		if previousMetrics.NumberOfNodes < TimeoutScalingThreshold {
			a.scalingFactor = TimeoutScalingStep
		} else {
			a.scalingFactor = 0
		}
	}

	// If I am scaling
	if a.scalingFactor != 0 && a.record == nil {
		// If I am starting to scale, prepare data point
		a.record = &predictor.AutoscalerData{
			Nodes: previousMetrics.NumberOfNodes,
			ScalingFactor: a.scalingFactor,
			PerformanceBefore: performance,
			MetricsBefore: MetricsToSnapshot(&previousMetrics),
		}
	} else {
		// If I have scaled, send data point
		a.record.MetricsAfter = MetricsToSnapshot(&previousMetrics)
		a.record.PerformanceAfter = performance
		// Send data point
		logrus.WithField("data", *a.record).Info("Sending autoscaler data to predictor")
		serverAddr := fmt.Sprintf("%s:%s",
			viper.GetString("predictorHost"),
			viper.GetString("predictorPort"))
		conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		pClient := predictor.NewObiPredictorClient(conn)
		pClient.CollectAutoscalerData(context.Background(), a.record)
		// Clear data point
		a.record = nil
	}

	return a.scalingFactor + previousMetrics.NumberOfNodes
}
