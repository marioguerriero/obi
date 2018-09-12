package policies

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"obi/master/model"
	"obi/master/predictor"
	"obi/master/utils"
	"time"
)

// TimeoutScalingStep constant value by which scale at each timeout
const TimeoutScalingStep = 15
// TimeoutLength number of metric windows to receive before scaling
const TimeoutLength = 2
// TimeoutPolicyUpperBound maximum number of scaling factor
const TimeoutPolicyUpperBound = 50

// TimeoutPolicy this policy scales the cluster each time it receives
// a certain amount of activations
type TimeoutPolicy struct {
	scalingFactor int32
	record        *predictor.AutoscalerData
	count		  int
}

// NewTimeout creates a new timeout policy for autoscaler
func NewTimeout() *TimeoutPolicy {
	// For later random number generation
	rand.Seed(time.Now().UTC().UnixNano())

	return &TimeoutPolicy{
		0,
		nil,
		TimeoutLength,
	}
}

// Apply scale based on a timeout: if it expires, add a node
func (p *TimeoutPolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.Metrics
	var throughput float32
	var pendingGrowthRate float32
	var count int8
	var performance float32

	logrus.Info("Applying timeout-based policy")
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
				pendingGrowth := float32(
					hb.PendingContainers - containersWillConsumed - previousMetrics.PendingContainers)
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

		fmt.Printf("Throughput: %f\n", throughput)
		fmt.Printf("Pending rate: %f\n", pendingGrowthRate)

		// Scale up one at each time interval until we reach p threshold
		if p.count == 0 && previousMetrics.NumberOfNodes < TimeoutPolicyUpperBound {
			p.scalingFactor = rand.Int31n(TimeoutScalingStep - 1) + 1
			if rand.Float32() < 0.5 {
				p.scalingFactor *= -1
			}
			p.count = TimeoutLength
		} else {
			p.scalingFactor = 0
		}

		// Never scale below the admitted threshold
		if previousMetrics.NumberOfNodes + p.scalingFactor < LowerBoundNodes {
			p.scalingFactor = 0
		}

		p.count--
	}

	if p.scalingFactor != 0 && p.record == nil {
		// Before scaling, save metrics
		p.record = &predictor.AutoscalerData{
			Nodes:             previousMetrics.NumberOfNodes,
			PerformanceBefore: performance,
			ScalingFactor:     p.scalingFactor,
			MetricsBefore:     MetricsToSnapshot(&previousMetrics),
		}
		logrus.WithField("data", p.record).Info("Created dataset record")
	}

	return p.scalingFactor
}
