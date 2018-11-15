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

package platforms

import (
	"obi/master/model"
		"github.com/sirupsen/logrus"
		"fmt"
)

// NewExistingCluster is a factory method to create one of the many platform instances when the resources are already
// allocated into the platform (e.g. Google Cloud, AWS, Azure)
// @param platform is the name of the cloud service
// @param clusterName is the name of the existing cluster inside that specific platform
func NewExistingCluster(platform string, clusterName string) (model.ClusterBaseInterface, error) {
	var newCluster model.ClusterBaseInterface
	var err error

	switch platform {
	case "dataproc":
		// TODO: Define config variables for Google Dataproc.
		newCluster, err = NewExistingDataprocCluster(
			"dhg-data-intelligence-ops",
			"global",
			"europe-west3-b",
			clusterName,
		)
	default:
		logrus.WithField("platform", platform).Error("Platform unknown")
		return nil, fmt.Errorf("impossible to create a new cluster for type '%s'", platform)
	}
	return newCluster, err
}
