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

// WorkloadPolicy contains all useful state-variable to apply the policy
type WorkloadPolicy struct {
	expCount int32
	record *predictor.AutoscalerData
	count int32
}

// NewWorkload is the constructor of the WorkloadPolicy struct
func NewWorkload() *WorkloadPolicy {
	return &WorkloadPolicy{
		record: nil,
		count: -1,
	}
}

// Apply is the implementation of the Policy interface
func (p *WorkloadPolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
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
			throughput += float32(hb.TotalContainersReleased - previousMetrics.TotalContainersReleased)

			if hb.PendingContainers > 0 {
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

		if p.record != nil {
			// If I have scaled, send data point
			p.record.MetricsAfter = MetricsToSnapshot(&previousMetrics)
			p.record.PerformanceAfter = performance
			// Send data point
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

		fmt.Printf("Throughput: %f\n", throughput)
		fmt.Printf("Pending rate: %f\n", pendingGrowthRate)
		if throughput < pendingGrowthRate {
			// scale up
			if p.expCount <= 0 {
				p.expCount = 1
			} else {
				p.expCount = p.expCount << 1
			}
		} else if (pendingGrowthRate == 0) || (throughput > pendingGrowthRate) {
			// scale down
			if p.expCount >= 0 {
				p.expCount = -1
			} else {
				p.expCount = p.expCount << 1
			}
		} else {
			p.expCount = 0
		}
		if p.expCount == 64 || p.expCount < -64 {
			p.expCount = 0
		}

		// Never scale below the admitted threshold
		if previousMetrics.NumberOfNodes + p.expCount < LowerBoundNodes {
			p.expCount = 0
		}
	}

	if p.expCount != 0 && p.record == nil {
		// Before scaling, save metrics
		p.record = &predictor.AutoscalerData{
			Nodes:             previousMetrics.NumberOfNodes,
			PerformanceBefore: performance,
			ScalingFactor:     p.expCount,
			MetricsBefore:     MetricsToSnapshot(&previousMetrics),
		}
	}

	return p.expCount
}
