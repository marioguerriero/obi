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

// GooglePolicy contains all useful state-variable to apply the policy
type GooglePolicy struct {
	record *predictor.AutoscalerData
}

// NewGoogle is the constructor of the GooglePolicy struct
func NewGoogle() *GooglePolicy {
	return &GooglePolicy{
		record: nil,
	}
}

// Apply is the implementation of the Policy interface
func (p *GooglePolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.Metrics
	var throughput float32
	var pendingGrowthRate float32
	var count int32
	var performance float32
	var scalingFactor int32
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

		previousMetrics = obj.Value.(model.Metrics)
	}

	if count > 0 {
		// Compute performances
		throughput /= float32(count)
		pendingGrowthRate /= float32(count)
		performance = throughput - pendingGrowthRate // I want to maximize this

		if p.record != nil {
			// If I have scaled, send data point
			p.record.MetricsAfter = MetricsToSnapshot(&previousMetrics)
			p.record.PerformanceAfter = performance
			// Send data point
			logrus.WithField("data", *p.record).Info("Sending autoscaler data to predictor")
			serverAddr := fmt.Sprintf("%s:%s",
				viper.GetString("predictorHost"),
				viper.GetString("predictorPort"))
			conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			pClient := predictor.NewObiPredictorClient(conn)
			pClient.CollectAutoscalerData(context.Background(), p.record)
			// Clear data point
			p.record = nil
		}

		// Check if we have to scale or not
		workers := float64(memoryUsage / count) / workerMemory
		fmt.Printf("Exact workers: %f\n", workers)
		scalingFactor = int32(workers) - previousMetrics.NumberOfNodes

		// Create autoscaler record
		if scalingFactor != 0 && p.record == nil {
			// Before scaling, save metrics
			p.record = &predictor.AutoscalerData{
				Nodes:             previousMetrics.NumberOfNodes,
				PerformanceBefore: performance,
				ScalingFactor:     scalingFactor,
				MetricsBefore:     MetricsToSnapshot(&previousMetrics),
			}
			logrus.WithField("data", p.record).Info("Created dataset record")
		}

		return scalingFactor
	}
	return 0
}
