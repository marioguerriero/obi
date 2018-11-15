// Copyright 2018 
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

package policies

import (
	"context"
	"fmt"
		"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"obi/master/model"
	"obi/master/predictor"
	"obi/master/utils"
)

// ExpWorkloadPolicy contains all useful state-variable to apply the policy
type ExpWorkloadPolicy struct {
	expCount int32
	record *predictor.AutoscalerData
}

// NewExpWorkload is the constructor of the WorkloadPolicy struct
func NewExpWorkload() *ExpWorkloadPolicy {
	return &ExpWorkloadPolicy{
		record: nil,
	}
}

// Apply is the implementation of the Policy interface
func (p *ExpWorkloadPolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
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
			MetricsBefore:     &previousMetrics,
		}
	}

	return p.expCount
}
