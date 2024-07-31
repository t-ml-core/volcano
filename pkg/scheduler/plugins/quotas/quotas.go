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
	"errors"
	"math"
	"math/rand"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	vcv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	PluginName = "quotas"

	ignoreNodeTaintKeysOpt = "ignore.node.taint.keys"
	ignoreNodeLabelsOpt    = "ignore.node.labels"

	allowedPercentageOfDeviationFromBestNodeScoreOpt = "allowed.percentage.of.deviation.from.best.node.score"
)

type quotasPlugin struct {
	totalQuotableResource     *api.Resource
	totalFreeQuotableResource *api.Resource

	totalGuarantee     *api.Resource
	totalFreeGuarantee *api.Resource

	queueOpts map[api.QueueID]*queueAttr

	ignoreTaintKeys  []string
	ignoreNodeLabels map[string][]string

	allowedPercentageOfDeviationFromBestNodeScoreOpt float64
}

type queueAttr struct {
	queueID api.QueueID
	name    string
	weight  int32

	allocated  *api.Resource
	preemption *api.Resource

	limit     *api.Resource
	guarantee *api.Resource
}

func (q *queueAttr) GetFreeGuarantee(adds ...*api.Resource) *api.Resource {
	newAllocated := q.allocated.Clone()
	for _, res := range adds {
		newAllocated = newAllocated.Add(res)
	}

	free, _ := q.guarantee.Diff(newAllocated, api.Zero)
	return free
}

/*
   - plugins:
     - name: quotas
       arguments:
         allowed.percentage.of.deviation.from.best.node.score: 0.1
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
		totalQuotableResource:     api.EmptyResource(),
		totalFreeQuotableResource: api.EmptyResource(),

		totalGuarantee:     api.EmptyResource(),
		totalFreeGuarantee: api.EmptyResource(),

		queueOpts: map[api.QueueID]*queueAttr{},

		ignoreTaintKeys:  []string{},
		ignoreNodeLabels: map[string][]string{},
		allowedPercentageOfDeviationFromBestNodeScoreOpt: 0.1,
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

	arguments.GetFloat64(&pp.allowedPercentageOfDeviationFromBestNodeScoreOpt, allowedPercentageOfDeviationFromBestNodeScoreOpt)

	klog.V(5).Infof("parsed quotas args %s: %v; %s: %v; %s: %v",
		ignoreNodeTaintKeysOpt, pp.ignoreTaintKeys,
		ignoreNodeLabelsOpt, pp.ignoreNodeLabels,
		allowedPercentageOfDeviationFromBestNodeScoreOpt, pp.allowedPercentageOfDeviationFromBestNodeScoreOpt,
	)

	return pp
}

func (p *quotasPlugin) Name() string {
	return PluginName
}

func (p *quotasPlugin) enableTaskInQuotas(info *api.TaskInfo) bool {
	if info.Preemptable {
		return false
	}

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

		guarantee: api.EmptyResource(),
		limit:     api.EmptyResource(),
	}

	if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
		attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
	}

	attr.limit.MilliCPU = math.MaxFloat64
	attr.limit.Memory = math.MaxFloat64
	if len(queue.Queue.Spec.Capability) != 0 {
		queueLimit := api.NewResource(queue.Queue.Spec.Capability)
		attr.limit = queueLimit.MinDimensionResource(attr.limit, api.Infinity)
	}

	realLimit := p.totalQuotableResource.Clone().Add(attr.guarantee).SubWithoutAssert(p.totalGuarantee)
	attr.limit = realLimit.MinDimensionResource(attr.limit, api.Infinity)

	return attr
}

var (
	errResourceReqIsGreaterThanLimit  = errors.New("job's resource request is greater than queue's limit")
	errResourceReqCanTakeSomeoneQuota = errors.New("job's resource request can take someone else's quota")
)

func (p *quotasPlugin) handleQuotas(attr *queueAttr, jobName string, resReq *api.Resource) error {
	guarantee := attr.guarantee.Clone()
	if p.totalGuarantee.MilliCPU == 0 {
		guarantee.MilliCPU = attr.limit.MilliCPU
	}

	if p.totalGuarantee.Memory == 0 {
		guarantee.Memory = attr.limit.Memory
	}

	for name := range attr.limit.ScalarResources {
		if _, ok := p.totalGuarantee.ScalarResources[name]; !ok {
			if guarantee.ScalarResources == nil {
				guarantee.ScalarResources = make(map[v1.ResourceName]float64)
			}

			guarantee.ScalarResources[name] = attr.limit.ScalarResources[name]
		}
	}

	incrAllocated := attr.allocated.Clone().Add(resReq)

	klog.V(4).Infof("job name <%s>; minResources <%v>; attr.limit <%v>; attr.guarantee <%v>; incrAllocated <%v>",
		jobName, resReq, attr.limit, guarantee, incrAllocated)

	if !incrAllocated.LessEqual(attr.limit, api.Zero) {
		return errResourceReqIsGreaterThanLimit
	}

	if incrAllocated.LessEqual(guarantee, api.Zero) {
		return nil
	}

	// totalFreeGuarantee - freeGuaranteeForCurrQueue + resReq <= totalFreeQuotableResource
	overGuarantee := p.totalFreeGuarantee.Clone().Add(resReq).Sub(attr.GetFreeGuarantee())
	if overGuarantee.LessEqual(p.totalFreeQuotableResource, api.Zero) {
		return nil
	}

	return errResourceReqCanTakeSomeoneQuota
}

func (p *quotasPlugin) OnSessionOpen(ssn *framework.Session) {
	for _, node := range ssn.Nodes {
		if p.enableNodeInQuotas(node) {
			p.totalQuotableResource.Add(node.Allocatable)
			p.totalFreeQuotableResource.Add(node.Idle)
		}
	}

	for _, queue := range ssn.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
			continue
		}
		guarantee := api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		p.totalGuarantee.Add(guarantee)
	}

	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		attr, found := p.queueOpts[job.Queue]
		if !found {
			queue := ssn.Queues[job.Queue]
			attr = p.createQueueAttr(queue)
		}

		for status, tasks := range job.TaskStatusIndex {
			for _, task := range tasks {
				if task.Preemptable {
					attr.preemption.Add(task.Resreq)
				}

				if !p.enableTaskInQuotas(task) {
					continue
				}

				if api.AllocatedStatus(status) {
					attr.allocated.Add(task.Resreq)
				}
			}
		}

		p.queueOpts[job.Queue] = attr
	}

	for _, queue := range ssn.Queues {
		attr, found := p.queueOpts[queue.UID]
		if !found {
			attr = p.createQueueAttr(queue)
			p.queueOpts[queue.UID] = attr
		}

		freeGuarantee := attr.GetFreeGuarantee()
		p.totalFreeGuarantee.Add(freeGuarantee)
		klog.V(4).Infof("free guarantee %v for queue %s", freeGuarantee, queue.Name)
	}

	klog.V(3).Infof("The total resource in quotas plugin <%v>, in cluster <%v>,"+
		"free <%v>, total guarantee resource is <%v>, total free guarantee resource is <%v>",
		p.totalQuotableResource, ssn.TotalResource, p.totalFreeQuotableResource, p.totalGuarantee, p.totalFreeGuarantee,
	)

	ssn.AddJobEnqueueableFn(p.Name(), func(obj any) int {
		job := obj.(*api.JobInfo)
		attr := p.queueOpts[job.Queue]

		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("Job %s MinResources is null.", job.Name)
			return util.Permit
		}

		for _, task := range job.Tasks {
			if !p.enableTaskInQuotas(task) {
				return util.Permit
			}
		}

		if err := p.handleQuotas(attr, job.Name, job.GetMinResources()); err != nil {
			if errors.Is(err, errResourceReqIsGreaterThanLimit) {
				ssn.SetJobPendingReason(job, p.Name(), vcv1beta1.NotEnoughResourcesInQuota, "EnqueueableFn: "+err.Error())
				return util.Reject
			}

			// we can free up resources through preemption
			klog.V(3).Infof("enqueueable warning with job `%s`:  %w.", job.Name, err)

			return util.Abstain
		}

		return util.Permit
	})

	ssn.AddAllocatableFn(p.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		if !p.enableTaskInQuotas(candidate) {
			return true
		}

		job := ssn.Jobs[candidate.Job]
		attr := p.queueOpts[queue.UID]

		if err := p.handleQuotas(attr, candidate.Name, candidate.Resreq); err != nil {
			ssn.SetJobPendingReason(job, p.Name(), vcv1beta1.NotEnoughResourcesInQuota, "AllocatableFn: "+err.Error())
			return false
		}

		return true
	})

	ssn.AddQueueOrderFn(p.Name(), func(l, r any) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		// The more preemptible tasks there are in the queue, the later we will process it.
		// This is necessary for the allocate-preempt loop to work more efficiently
		if !p.queueOpts[lv.UID].preemption.Equal(p.queueOpts[rv.UID].preemption, api.Zero) {
			if p.queueOpts[lv.UID].preemption.LessEqual(p.queueOpts[rv.UID].preemption, api.Zero) {
				return -1
			}

			return 1
		}

		return 0
	})

	ssn.AddBestNodeFn(p.Name(), func(task *api.TaskInfo, nodeScores map[float64][]*api.NodeInfo) *api.NodeInfo {
		if task.Preemptable {
			return nil
		}

		var scores []float64
		for score := range nodeScores {
			scores = append(scores, score)
		}

		if len(scores) == 0 {
			return nil
		}

		sort.Slice(scores, func(i, j int) bool {
			return scores[i] > scores[j]
		})

		maxScore := scores[0]
		if maxScore == 0 {
			maxScore = 1
		}

		for _, score := range scores {
			if 1.0-score/maxScore > p.allowedPercentageOfDeviationFromBestNodeScoreOpt {
				continue
			}

			var bestNodes []*api.NodeInfo
			for _, node := range nodeScores[score] {
				if task.InitResreq.LessEqual(node.Idle, api.Zero) {
					bestNodes = append(bestNodes, node)
				}
			}

			if len(bestNodes) > 0 {
				return bestNodes[rand.Intn(len(bestNodes))]
			}
		}

		return nil
	})

	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			if node := ssn.Nodes[event.Task.NodeName]; node != nil && p.enableNodeInQuotas(node) {
				p.totalFreeQuotableResource.SubWithoutAssert(event.Task.Resreq)
			}

			if !p.enableTaskInQuotas(event.Task) {
				return
			}

			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				return
			}

			attr := p.queueOpts[job.Queue]

			p.totalFreeGuarantee.Sub(attr.GetFreeGuarantee())
			attr.allocated.Add(event.Task.Resreq)
			p.totalFreeGuarantee.Add(attr.GetFreeGuarantee())

			klog.V(4).Infof("Quotas AllocateFunc: task <%v/%v>, resreq <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq)
		},
		DeallocateFunc: func(event *framework.Event) {
			if node := ssn.Nodes[event.Task.NodeName]; node != nil && p.enableNodeInQuotas(node) {
				p.totalFreeQuotableResource.Add(event.Task.Resreq)
			}

			if !p.enableTaskInQuotas(event.Task) {
				return
			}

			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				return
			}

			attr := p.queueOpts[job.Queue]

			p.totalFreeGuarantee.Sub(attr.GetFreeGuarantee())
			attr.allocated.Sub(event.Task.Resreq)
			p.totalFreeGuarantee.Add(attr.GetFreeGuarantee())

			klog.V(4).Infof("Quotas DeallocateFunc: task <%v/%v>, resreq <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq)
		},
	})
}

func (p *quotasPlugin) OnSessionClose(ssn *framework.Session) {
}
