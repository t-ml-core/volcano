/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package predicates

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const TaskIgnoreProportionAnnotation = "volcano.sh/ignore-proportion"

func shouldIgnoreProportion(task *api.TaskInfo) bool {
	if task.Pod == nil {
		return false
	}

	val, found := task.Pod.Annotations[TaskIgnoreProportionAnnotation]
	if !found {
		return false
	}

	ignore, err := strconv.ParseBool(val)
	if err != nil {
		return false
	}

	return ignore
}

// checkNodeResourceIsProportional checks if a gpu:cpu:memory is Proportional
func checkNodeResourceIsProportional(task *api.TaskInfo, node *api.NodeInfo, proportional map[v1.ResourceName]baseResource) (bool, error) {
	for resourceName := range proportional {
		if value, found := task.Resreq.ScalarResources[resourceName]; found && value > 0 {
			return true, nil
		}
	}

	if shouldIgnoreProportion(task) {
		return true, nil
	}

	for resourceName, resourceRate := range proportional {
		if value, found := node.Idle.ScalarResources[resourceName]; found {
			cpuReserved := value * resourceRate.CPU
			memoryReserved := value * resourceRate.Memory * 1000 * 1000

			if node.Idle.MilliCPU-task.Resreq.MilliCPU < cpuReserved || node.Idle.Memory-task.Resreq.Memory < memoryReserved {
				return false, fmt.Errorf("proportional of resource %s check failed", resourceName)
			}
		}
	}
	return true, nil
}
