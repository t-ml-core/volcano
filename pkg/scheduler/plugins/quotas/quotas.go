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
	"fmt"
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

	totalGuaranteeResource         *api.Resource
	totalFreeGuaranteeResource     *api.Resource
	totalActivePreemptibleResource *api.Resource

	queueOpts map[api.QueueID]*queueAttr

	ignoreTaintKeys  []string
	ignoreNodeLabels map[string][]string

	allowedPercentageOfDeviationFromBestNodeScoreOpt float64
}

type queueAttr struct {
	queueID api.QueueID
	name    string
	weight  int32

	allocated   *api.Resource
	preemptible *api.Resource

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

		totalGuaranteeResource:         api.EmptyResource(),
		totalFreeGuaranteeResource:     api.EmptyResource(),
		totalActivePreemptibleResource: api.EmptyResource(),

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

		allocated:   api.EmptyResource(),
		preemptible: api.EmptyResource(),

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
		if queueLimit.MilliCPU == 0 {
			queueLimit.MilliCPU = math.MaxFloat64
		}
		if queueLimit.Memory == 0 {
			queueLimit.Memory = math.MaxFloat64
		}

		attr.limit = queueLimit.MinDimensionResource(attr.limit, api.Infinity)
	}

	realLimit := p.totalQuotableResource.Clone().Add(attr.guarantee).SubWithoutAssert(p.totalGuaranteeResource)
	attr.limit = realLimit.MinDimensionResource(attr.limit, api.Infinity)

	return attr
}

var (
	errResourceReqIsGreaterThanLimit  = errors.New("job's resource request is greater than queue's limit")
	errResourceReqCanTakeSomeoneQuota = errors.New("job's resource request can take someone else's quota")
)

func (p *quotasPlugin) handleQuotas(attr *queueAttr, jobName string, resReq *api.Resource) error {
	guarantee := attr.guarantee.Clone()
	if p.totalGuaranteeResource.MilliCPU == 0 {
		guarantee.MilliCPU = attr.limit.MilliCPU
	}

	if p.totalGuaranteeResource.Memory == 0 {
		guarantee.Memory = attr.limit.Memory
	}

	for name := range attr.limit.ScalarResources {
		if _, ok := p.totalGuaranteeResource.ScalarResources[name]; !ok {
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

	overGuarantee := p.totalFreeGuaranteeResource.Clone().Add(resReq).Sub(attr.GetFreeGuarantee())

	if !resReq.IsEmpty() {
		for name := range overGuarantee.ScalarResources {
			if _, ok := resReq.ScalarResources[name]; !ok {
				delete(overGuarantee.ScalarResources, name)
			}
		}
	}

	// totalFreeGuarantee - freeGuaranteeForCurrQueue + resReq <= totalFreeQuotableResource + totalActivePreemptible
	if overGuarantee.LessEqual(p.totalFreeQuotableResource.Clone().Add(p.totalActivePreemptibleResource), api.Zero) {
		return nil
	}

	return fmt.Errorf("%w; overGuarantee: %v; resReq: %v; attr.allocated: %v; totalActivePreemptible: %v; totalFreeGuarantee: %v; totalFreeQuotableResource: %v",
		errResourceReqCanTakeSomeoneQuota,
		overGuarantee,
		resReq,
		attr.allocated,
		p.totalActivePreemptibleResource,
		p.totalFreeGuaranteeResource,
		p.totalFreeQuotableResource,
	)
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
		p.totalGuaranteeResource.Add(guarantee)
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
					attr.preemptible.Add(task.Resreq)

					if api.AllocatedStatus(status) {
						p.totalActivePreemptibleResource.Add(task.Resreq)
					}
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
		p.totalFreeGuaranteeResource.Add(freeGuarantee)
		klog.V(4).Infof("free guarantee %v for queue %s", freeGuarantee, queue.Name)
	}

	klog.V(3).Infof("The total resource in quotas plugin <%v>, in cluster <%v>,"+
		"free <%v>, total guarantee resource is <%v>, total free guarantee resource is <%v>",
		p.totalQuotableResource, ssn.TotalResource, p.totalFreeQuotableResource, p.totalGuaranteeResource, p.totalFreeGuaranteeResource,
	)

	// Enqueueable override does not work and is disabled in the config
	// more details: https://github.com/volcano-sh/volcano/issues/3661
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
			reason := vcv1beta1.NotEnoughResourcesInQuota
			if errors.Is(err, errResourceReqCanTakeSomeoneQuota) {
				reason = vcv1beta1.NotEnoughResourcesInCluster
			}
			ssn.SetJobPendingReason(job, p.Name(), reason, "AllocatableFn: "+err.Error())
			return false
		}

		return true
	})

	ssn.AddOverusedFn(p.Name(), func(obj any) (bool, *api.OverusedInfo) {
		queue := obj.(*api.QueueInfo)
		attr := p.queueOpts[queue.UID]

		if err := p.handleQuotas(attr, "overused-"+queue.Name, api.EmptyResource()); err != nil {
			reason := vcv1beta1.NotEnoughResourcesInQuota
			if errors.Is(err, errResourceReqCanTakeSomeoneQuota) {
				reason = vcv1beta1.NotEnoughResourcesInCluster
			}

			return true, &api.OverusedInfo{
				Plugin:  p.Name(),
				Reason:  string(reason),
				Message: "OverusedFn: " + err.Error(),
			}
		}

		return false, nil
	})

	ssn.AddQueueOrderFn(p.Name(), func(l, r any) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		// The more preemptible tasks there are in the queue, the later we will process it.
		// This is necessary for the allocate-preempt loop to work more efficiently
		if !p.queueOpts[lv.UID].preemptible.Equal(p.queueOpts[rv.UID].preemptible, api.Zero) {
			if p.queueOpts[lv.UID].preemptible.LessEqual(p.queueOpts[rv.UID].preemptible, api.Zero) {
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
			if taskNode := ssn.Nodes[event.Task.NodeName]; taskNode != nil && p.enableNodeInQuotas(taskNode) {
				p.totalFreeQuotableResource = api.EmptyResource()
				for _, node := range ssn.Nodes {
					if p.enableNodeInQuotas(node) {
						p.totalFreeQuotableResource.Add(node.FutureIdle())
					}
				}
			}

			klog.V(3).Infof("Quotas AllocateFunc: task <%v/%v>, resreq <%v>, totalFreeGuarantee: <%v>, totalFreeQuotableResource: <%v>",
				event.Task.Namespace,
				event.Task.Name,
				event.Task.Resreq,
				p.totalFreeGuaranteeResource,
				p.totalFreeQuotableResource,
			)

			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				return
			}

			attr := p.queueOpts[job.Queue]

			if event.Task.Preemptable {
				p.totalActivePreemptibleResource.Add(event.Task.Resreq)
			}

			if !p.enableTaskInQuotas(event.Task) {
				return
			}

			p.totalFreeGuaranteeResource.Sub(attr.GetFreeGuarantee())
			attr.allocated.Add(event.Task.Resreq)
			p.totalFreeGuaranteeResource.Add(attr.GetFreeGuarantee())
		},
		DeallocateFunc: func(event *framework.Event) {
			if taskNode := ssn.Nodes[event.Task.NodeName]; taskNode != nil && p.enableNodeInQuotas(taskNode) {
				p.totalFreeQuotableResource = api.EmptyResource()
				for _, node := range ssn.Nodes {
					if p.enableNodeInQuotas(node) {
						p.totalFreeQuotableResource.Add(node.FutureIdle())
					}
				}
			}

			klog.V(3).Infof("Quotas DeallocateFunc: task <%v/%v>, resreq <%v>, totalFreeGuarantee: <%v>, totalFreeQuotableResource: <%v>",
				event.Task.Namespace,
				event.Task.Name,
				event.Task.Resreq,
				p.totalFreeGuaranteeResource,
				p.totalFreeQuotableResource,
			)

			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				return
			}

			attr := p.queueOpts[job.Queue]

			if event.Task.Preemptable {
				p.totalActivePreemptibleResource.SubWithoutAssert(event.Task.Resreq)
			}

			if !p.enableTaskInQuotas(event.Task) {
				return
			}

			p.totalFreeGuaranteeResource.Sub(attr.GetFreeGuarantee())
			attr.allocated.Sub(event.Task.Resreq)
			p.totalFreeGuaranteeResource.Add(attr.GetFreeGuarantee())
		},
	})
}

func (p *quotasPlugin) OnSessionClose(ssn *framework.Session) {
}
