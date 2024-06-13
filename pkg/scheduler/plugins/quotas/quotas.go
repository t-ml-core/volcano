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

package quotas

import (
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	PluginName = "quotas"

	ignoreNodeTaintKeysOpt = "ignore.node.taint.keys"
	ignoreNodeLabelsOpt    = "ignore.node.labels"

	allowedDeltaFromBestNodeScoreOpt = "allowed-delta-from-best-node-score"
)

type quotasPlugin struct {
	totalQuotaResource *api.Resource
	totalGuarantee     *api.Resource

	queueOpts map[api.QueueID]*queueAttr

	ignoreTaintKeys  []string
	ignoreNodeLabels map[string][]string

	allowedDeltaFromBestNodeScore float64
}

type queueAttr struct {
	queueID api.QueueID
	name    string
	weight  int32

	allocated  *api.Resource
	preemption *api.Resource

	capability *api.Resource
	guarantee  *api.Resource
}

/*
   - plugins:
     - name: quotas
       arguments:
         allowed-delta-from-best-node-score: 0.1
         ignore.node.taint.keys:
            - node.kubernetes.io/unschedulable
            - ...
         ignore.node.labels:
           kubernetes.io/hostname:
             - host1
             - host2
             - ...
           ...
*/

// New return quotas action
func New(arguments framework.Arguments) framework.Plugin {
	pp := &quotasPlugin{
		totalQuotaResource: api.EmptyResource(),
		totalGuarantee:     api.EmptyResource(),
		queueOpts:          map[api.QueueID]*queueAttr{},

		ignoreTaintKeys:               []string{},
		ignoreNodeLabels:              map[string][]string{},
		allowedDeltaFromBestNodeScore: 0.1,
	}

	if ignoreNodeTaintKeysI, ok := arguments[ignoreNodeTaintKeysOpt]; ok {
		if ignoreNodeTaintKeys, ok := ignoreNodeTaintKeysI.([]any); ok {
			for _, taintKeyI := range ignoreNodeTaintKeys {
				if taintKey, ok := taintKeyI.(string); ok {
					pp.ignoreTaintKeys = append(pp.ignoreTaintKeys, taintKey)
				}
			}
		}
	}

	if ignoreNodeLabelsI, ok := arguments[ignoreNodeLabelsOpt]; ok {
		if ignoreNodeLabels, ok := ignoreNodeLabelsI.(map[any]any); ok {
			for labelNameI, labelValuesI := range ignoreNodeLabels {
				labelName, ok := labelNameI.(string)
				if !ok {
					continue
				}

				if _, ok := pp.ignoreNodeLabels[labelName]; !ok {
					pp.ignoreNodeLabels[labelName] = []string{}
				}

				labelValues, ok := labelValuesI.([]any)
				if !ok {
					continue
				}

				for _, labelValueI := range labelValues {
					if labelValue, ok := labelValueI.(string); ok {
						pp.ignoreNodeLabels[labelName] = append(pp.ignoreNodeLabels[labelName], labelValue)
					}
				}
			}
		}
	}

	arguments.GetFloat64(&pp.allowedDeltaFromBestNodeScore, allowedDeltaFromBestNodeScoreOpt)

	klog.V(5).Infof("parsed quotas args %s: %v; %s: %v; %s: %v",
		ignoreNodeTaintKeysOpt, pp.ignoreTaintKeys,
		ignoreNodeLabelsOpt, pp.ignoreNodeLabels,
		allowedDeltaFromBestNodeScoreOpt, pp.allowedDeltaFromBestNodeScore,
	)

	return pp
}

func (p *quotasPlugin) Name() string {
	return PluginName
}

func (p *quotasPlugin) enableTaskInQuotas(info *api.TaskInfo) bool {
	if info.Pod == nil {
		return true
	}

	for selectorName, selectorValue := range info.Pod.Spec.NodeSelector {
		ignoreValues, ok := p.ignoreNodeLabels[selectorName]
		if !ok {
			continue
		}

		for _, ignoreValue := range ignoreValues {
			if selectorValue == ignoreValue {
				klog.V(3).Infof("ignore task %s in quotas plugin by node selector %s", info.Name, selectorName)
				return false
			}
		}
	}

	if info.Pod.Spec.Affinity == nil ||
		info.Pod.Spec.Affinity.NodeAffinity == nil ||
		info.Pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}

	terms := info.Pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms

	// task has node affinity on ignore node
	for _, term := range terms {
		for _, expression := range term.MatchExpressions {
			if expression.Operator != v1.NodeSelectorOpIn {
				continue
			}

			ignoreValues, ok := p.ignoreNodeLabels[expression.Key]
			if !ok {
				continue
			}

			for _, taskExpressionValue := range expression.Values {
				for _, ignoreValue := range ignoreValues {
					if taskExpressionValue == ignoreValue {
						klog.V(3).Infof("ignore task %s in quotas plugin by affinity %s", info.Name, expression.Key)
						return false
					}
				}
			}
		}
	}

	return true
}

func (p *quotasPlugin) enableNodeInQuotas(node *api.NodeInfo) bool {
	if !node.Ready() {
		return false
	}

	for _, taint := range node.Node.Spec.Taints {
		for _, ignoreTaintKey := range p.ignoreTaintKeys {
			if taint.Key == ignoreTaintKey {
				klog.V(3).Infof("ignore node %s in quotas plugin by taint %s", node.Name, taint.Key)
				return false
			}
		}
	}

	for name, value := range node.Node.Labels {
		ignoreValues, ok := p.ignoreNodeLabels[name]
		if !ok {
			continue
		}

		for _, ignoreValue := range ignoreValues {
			if value == ignoreValue {
				klog.V(3).Infof("ignore node %s in quotas plugin by label %s with value %s", node.Name, name, value)
				return false
			}
		}
	}

	return true
}

func (p *quotasPlugin) createQueueAttr(queue *api.QueueInfo) *queueAttr {
	attr := &queueAttr{
		queueID: queue.UID,
		name:    queue.Name,
		weight:  queue.Weight,

		allocated:  api.EmptyResource(),
		preemption: api.EmptyResource(),

		capability: api.EmptyResource(),
		guarantee:  api.EmptyResource(),
	}

	return attr
}

func (p *quotasPlugin) OnSessionOpen(ssn *framework.Session) {
	for _, node := range ssn.Nodes {
		if p.enableNodeInQuotas(node) {
			p.totalQuotaResource.Add(node.Allocatable)
		}
	}
	klog.V(4).Infof("The total resource in quotas plugin <%v>, in cluster <%v>", p.totalQuotaResource, ssn.TotalResource)

	for _, queue := range ssn.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
			continue
		}
		guarantee := api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		p.totalGuarantee.Add(guarantee)
	}
	klog.V(4).Infof("The total guarantee resource is <%v>", p.totalGuarantee)

	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		attr, found := p.queueOpts[job.Queue]
		if !found {
			queue := ssn.Queues[job.Queue]
			attr = p.createQueueAttr(queue)
		}

		p.queueOpts[job.Queue] = attr
	}

}

func (p *quotasPlugin) OnSessionClose(ssn *framework.Session) {
}
