package policies

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"math"
	"obi/master/model"
	"obi/master/predictor"
	"obi/master/utils"
)

// ScalingTrigger integer constant used to decide when to trigger autoscaler
const ScalingTrigger = 6

// MLPolicy contains all useful state-variable to apply the policy
type MLPolicy struct {
	scalingFactor int32
	record        *predictor.AutoscalerData
	client        predictor.ObiPredictorClient
}

// NewExpWorkload is the constructor of the WorkloadPolicy struct
func NewMLPolicy() *MLPolicy {
	// Open predictor connection
	serverAddr := fmt.Sprintf("%s:%s",
		viper.GetString("predictorHost"),
		viper.GetString("predictorPort"))
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	return &MLPolicy{
		record: nil,
		client: predictor.NewObiPredictorClient(conn),
	}
}

// Apply is the implementation of the Policy interface
func (p *MLPolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.HeartbeatMessage
	var throughput float32
	var pendingGrowthRate float32
	var count int8
	var performance float32

	for obj := range metricsWindow.Iter() {
		if obj.Value == nil {
			continue
		}

		hb := obj.Value.(model.HeartbeatMessage)

		if previousMetrics.ClusterName != "" {
			throughput += float32(hb.AggregateContainersReleased - previousMetrics.AggregateContainersReleased)
			if hb.PendingContainers > 0 {
				memoryContainer := hb.PendingMB / hb.PendingContainers
				containersWillConsumed := hb.AvailableMB / memoryContainer
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
			p.record.MetricsAfter = &previousMetrics
			p.record.PerformanceAfter = performance
			p.client.CollectAutoscalerData(context.Background(), p.record)
			// Clear data point
			p.record = nil
		}

		fmt.Printf("Throughput: %f\n", throughput)
		fmt.Printf("Pending rate: %f\n", pendingGrowthRate)

		// Decide whether to scale or not
		if math.Abs(float64(performance)) > ScalingTrigger {
			scalingResp, err := p.client.RequestPrediction(context.Background(),
				&predictor.AutoscalerRequest{
					Metrics: previousMetrics,
					Performance: performance,
				},
			)
			if err != nil {
				logrus.WithField("error", err).Error("MLAutoscaler could not generate predictions")
				p.scalingFactor = 0
			} else {
				p.scalingFactor = scalingResp.scalingFactor
			}
		}

		// Never scale below the admitted threshold
		if previousMetrics.NumberOfNodes + p.scalingFactor < LowerBoundNodes {
			p.scalingFactor = 0
		}
	}

	if p.scalingFactor != 0 && p.record == nil {
		// Before scaling, save metrics
		p.record = &predictor.AutoscalerData{
			Nodes:             previousMetrics.NumberOfNodes,
			PerformanceBefore: performance,
			ScalingFactor:     p.scalingFactor,
			MetricsBefore:     &previousMetrics,
		}
	}

	return p.scalingFactor
}
