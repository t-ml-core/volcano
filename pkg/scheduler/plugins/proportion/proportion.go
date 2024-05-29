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

package proportion

import (
	"math"
	"math/rand"
	"reflect"
	"sort"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	vcv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "proportion"

	ignoreNodeTaintKeysOpt = "ignore.node.taint.keys"
	ignoreNodeLabelsOpt    = "ignore.node.labels"

	allowedDeltaFromBestNodeScoreOpt = "allowed.delta.from.best.node.score"
)

type proportionPlugin struct {
	totalResource              *api.Resource
	totalGuarantee             *api.Resource
	totalNotAllocatedResources *api.Resource
	queueOpts                  map[api.QueueID]*queueAttr

	ignoreTaintKeys  []string
	ignoreNodeLabels map[string][]string

	allowedDeltaFromBestNodeScore float64

	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

type queueAttr struct {
	queueID api.QueueID
	name    string
	weight  int32
	share   float64

	deserved  *api.Resource
	allocated *api.Resource
	request   *api.Resource
	// preemption represents the sum of preemtable jobs
	preemption *api.Resource
	// elastic represents the sum of job's elastic resource, job's elastic = job.allocated - job.minAvailable
	elastic *api.Resource
	// inqueue represents the resource request of the inqueue job
	inqueue    *api.Resource
	capability *api.Resource
	// realCapability represents the resource limit of the queue, LessEqual capability
	realCapability *api.Resource
	guarantee      *api.Resource
}

/*
   - plugins:
     - name: proportion
       arguments:
         allowed.delta.from.best.node.score: 0.1
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

// New return proportion action
func New(arguments framework.Arguments) framework.Plugin {
	pp := &proportionPlugin{
		totalResource:              api.EmptyResource(),
		totalGuarantee:             api.EmptyResource(),
		totalNotAllocatedResources: api.EmptyResource(),
		queueOpts:                  map[api.QueueID]*queueAttr{},

		ignoreTaintKeys:               []string{},
		ignoreNodeLabels:              map[string][]string{},
		allowedDeltaFromBestNodeScore: 0.1,

		pluginArguments: arguments,
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

	klog.V(5).Infof("parsed proportion args %s: %v; %s: %v; %s: %v",
		ignoreNodeTaintKeysOpt, pp.ignoreTaintKeys,
		ignoreNodeLabelsOpt, pp.ignoreNodeLabels,
		allowedDeltaFromBestNodeScoreOpt, pp.allowedDeltaFromBestNodeScore,
	)

	return pp
}

func (pp *proportionPlugin) Name() string {
	return PluginName
}

func (pp *proportionPlugin) enableTaskInProportion(info *api.TaskInfo) bool {
	if info.Pod == nil {
		return true
	}

	for selectorName, selectorValue := range info.Pod.Spec.NodeSelector {
		ignoreValues, ok := pp.ignoreNodeLabels[selectorName]
		if !ok {
			continue
		}

		for _, ignoreValue := range ignoreValues {
			if selectorValue == ignoreValue {
				klog.V(3).Infof("ignore task %s in proportion plugin by node selector %s", info.Name, selectorName)
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

			ignoreValues, ok := pp.ignoreNodeLabels[expression.Key]
			if !ok {
				continue
			}

			for _, taskExpressionValue := range expression.Values {
				for _, ignoreValue := range ignoreValues {
					if taskExpressionValue == ignoreValue {
						klog.V(3).Infof("ignore task %s in proportion plugin by affinity %s", info.Name, expression.Key)
						return false
					}
				}
			}
		}
	}

	return true
}

func (pp *proportionPlugin) enableNodeInProportion(node *api.NodeInfo) bool {
	if !node.Ready() {
		return false
	}

	for _, taint := range node.Node.Spec.Taints {
		for _, ignoreTaintKey := range pp.ignoreTaintKeys {
			if taint.Key == ignoreTaintKey {
				klog.V(3).Infof("ignore node %s in proportion plugin by taint %s", node.Name, taint.Key)
				return false
			}
		}
	}

	for name, value := range node.Node.Labels {
		ignoreValues, ok := pp.ignoreNodeLabels[name]
		if !ok {
			continue
		}

		for _, ignoreValue := range ignoreValues {
			if value == ignoreValue {
				klog.V(3).Infof("ignore node %s in proportion plugin by label %s with value %s", node.Name, name, value)
				return false
			}
		}
	}

	return true
}

func (pp *proportionPlugin) calculateTotalResources() {
	// We allow execution of preemptable tasks over guaranteed resources,
	// so we need to understand how many resources are available in a cluster
	totalAllocatedResources := api.EmptyResource()
	for _, attr := range pp.queueOpts {
		totalAllocatedResources.Add(attr.allocated)
	}

	// TODO: this crunch should be replaced with the following logic:
	// should calculate totalNotAllocatedResources as follows: walk all
	// allocated tasks and if a node doesn't have gpu resources, add gpu resources from the task
	// otherwise if node has gpu resources, should subtract resources. Scheduler actions will believe that these
	// gpus are available and may schedule another task, then it would be panic in the AllocateFunc
	pp.totalResource.SetMaxResource(totalAllocatedResources)

	pp.totalNotAllocatedResources = pp.totalResource.Clone().Sub(totalAllocatedResources)
}

func (pp *proportionPlugin) OnSessionOpen(ssn *framework.Session) {
	// Prepare scheduling data for this session.
	for _, node := range ssn.Nodes {
		if pp.enableNodeInProportion(node) {
			pp.totalResource.Add(node.Allocatable)
		}
	}

	klog.V(4).Infof("The total resource in proportion plugin <%v>, in cluster <%v>", pp.totalResource, ssn.TotalResource)
	for _, queue := range ssn.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
			continue
		}
		guarantee := api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		pp.totalGuarantee.Add(guarantee)
	}
	klog.V(4).Infof("The total guarantee resource is <%v>", pp.totalGuarantee)
	// Build attributes for Queues.
	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		if _, found := pp.queueOpts[job.Queue]; !found {
			queue := ssn.Queues[job.Queue]
			attr := &queueAttr{
				queueID: queue.UID,
				name:    queue.Name,
				weight:  queue.Weight,

				deserved:   api.EmptyResource(),
				allocated:  api.EmptyResource(),
				request:    api.EmptyResource(),
				preemption: api.EmptyResource(),
				elastic:    api.EmptyResource(),
				inqueue:    api.EmptyResource(),
				guarantee:  api.EmptyResource(),
			}
			if len(queue.Queue.Spec.Capability) != 0 {
				attr.capability = api.NewResource(queue.Queue.Spec.Capability)
				if attr.capability.MilliCPU <= 0 {
					attr.capability.MilliCPU = math.MaxFloat64
				}
				if attr.capability.Memory <= 0 {
					attr.capability.Memory = math.MaxFloat64
				}
			}
			if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
				attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
			}
			realCapability := pp.totalResource.Clone().Sub(pp.totalGuarantee).Add(attr.guarantee)
			if attr.capability == nil {
				attr.realCapability = realCapability
			} else {
				realCapability.MinDimensionResource(attr.capability, api.Infinity)
				attr.realCapability = realCapability
			}
			pp.queueOpts[job.Queue] = attr
			klog.V(4).Infof("Added Queue <%s> attributes.", job.Queue)
		}

		attr := pp.queueOpts[job.Queue]
		for status, tasks := range job.TaskStatusIndex {
			for _, task := range tasks {
				if task.Preemptable {
					attr.preemption.Add(task.Resreq)
				}

				if !pp.enableTaskInProportion(task) {
					continue
				}

				if api.AllocatedStatus(status) {
					attr.allocated.Add(task.Resreq)
					attr.request.Add(task.Resreq)
				} else if status == api.Pending {
					attr.request.Add(task.Resreq)
				}
			}
		}

		pp.calculateTotalResources()

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			attr.inqueue.Add(job.GetMinResources())
		}

		// calculate inqueue resource for running jobs
		// the judgement 'job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember' will work on cases such as the following condition:
		// Considering a Spark job is completed(driver pod is completed) while the podgroup keeps running, the allocated resource will be reserved again if without the judgement.
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			allocated := util.GetAllocatedResource(job)
			inqueued := util.GetInqueueResource(job, allocated)
			attr.inqueue.Add(inqueued)
		}
		attr.elastic.Add(job.GetElasticResources())
		klog.V(5).Infof("Queue %s allocated <%s> request <%s> inqueue <%s> elastic <%s>",
			attr.name, attr.allocated.String(), attr.request.String(), attr.inqueue.String(), attr.elastic.String())
	}

	for queueID, queueInfo := range ssn.Queues {
		if _, ok := pp.queueOpts[queueID]; !ok {
			metrics.UpdateQueueAllocated(queueInfo.Name, 0, 0, 0)
		}
	}

	// Record metrics
	for _, attr := range pp.queueOpts {
		metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Get(api.GPUResourceName), attr.allocated.Memory)
		metrics.UpdateQueueRequest(attr.name, attr.request.MilliCPU, attr.request.Memory)
		metrics.UpdateQueueWeight(attr.name, attr.weight)
		queue := ssn.Queues[attr.queueID]
		metrics.UpdateQueuePodGroupInqueueCount(attr.name, queue.Queue.Status.Inqueue)
		metrics.UpdateQueuePodGroupPendingCount(attr.name, queue.Queue.Status.Pending)
		metrics.UpdateQueuePodGroupRunningCount(attr.name, queue.Queue.Status.Running)
		metrics.UpdateQueuePodGroupUnknownCount(attr.name, queue.Queue.Status.Unknown)
	}

	remaining := pp.totalResource.Clone()

	// Process guarantee resources of the queues
	// We must substract the guarantees from the remaining before
	// fairness divison of the remianing
	for _, attr := range pp.queueOpts {
		// Queue can't desrve less resources than it has in guarantee
		if attr.deserved.LessEqual(attr.guarantee, api.Zero) {
			guarantee := attr.guarantee.Clone()
			attr.deserved = guarantee
			remaining.Sub(guarantee)
		}
	}

	meet := map[api.QueueID]struct{}{}
	for {
		totalWeight := int32(0)
		for _, attr := range pp.queueOpts {
			if _, found := meet[attr.queueID]; found {
				continue
			}
			totalWeight += attr.weight
		}

		// If no queues, break
		if totalWeight == 0 {
			klog.V(4).Infof("Exiting when total weight is 0")
			break
		}

		oldRemaining := remaining.Clone()
		// Calculates the deserved of each Queue.
		// increasedDeserved is the increased value for attr.deserved of processed queues
		// decreasedDeserved is the decreased value for attr.deserved of processed queues
		increasedDeserved := api.EmptyResource()
		decreasedDeserved := api.EmptyResource()
		for _, attr := range pp.queueOpts {
			klog.V(4).Infof("Considering Queue <%s>: weight <%d>, total weight <%d>.",
				attr.name, attr.weight, totalWeight)
			if _, found := meet[attr.queueID]; found {
				continue
			}

			oldDeserved := attr.deserved.Clone()
			attr.deserved.Add(remaining.Clone().Multi(float64(attr.weight) / float64(totalWeight)))

			if attr.realCapability != nil {
				attr.deserved.MinDimensionResource(attr.realCapability, api.Infinity)
			}
			attr.deserved.MinDimensionResource(attr.request, api.Zero)

			// attr.requests or attr.realCapability can be less then guarantee,
			// but queue can't deserve less resources than it has in guarantee
			attr.deserved = helpers.Max(attr.deserved, attr.guarantee)
			pp.updateShare(attr)
			klog.V(4).Infof("Format queue <%s> deserved resource to <%v>", attr.name, attr.deserved)

			if attr.request.LessEqual(attr.deserved, api.Zero) {
				meet[attr.queueID] = struct{}{}
				klog.V(4).Infof("queue <%s> is meet", attr.name)
			} else if reflect.DeepEqual(attr.deserved, oldDeserved) {
				meet[attr.queueID] = struct{}{}
				klog.V(4).Infof("queue <%s> is meet cause of the capability", attr.name)
			}

			klog.V(4).Infof("The attributes of queue <%s> in proportion: deserved <%v>, realCapability <%v>, allocate <%v>, request <%v>, elastic <%v>, share <%0.2f>",
				attr.name, attr.deserved, attr.realCapability, attr.allocated, attr.request, attr.elastic, attr.share)

			increased, decreased := attr.deserved.Diff(oldDeserved, api.Zero)
			increasedDeserved.Add(increased)
			decreasedDeserved.Add(decreased)

			// Record metrics
			metrics.UpdateQueueDeserved(attr.name, attr.deserved.MilliCPU, attr.deserved.Memory)
		}

		remaining.Sub(increasedDeserved).Add(decreasedDeserved)
		klog.V(4).Infof("Remaining resource is  <%s>", remaining)
		if remaining.IsEmpty() || reflect.DeepEqual(remaining, oldRemaining) {
			klog.V(4).Infof("Exiting when remaining is empty or no queue has more resource request:  <%v>", remaining)
			break
		}
	}

	ssn.AddQueueOrderFn(pp.Name(), func(l, r interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		if !pp.queueOpts[lv.UID].preemption.Equal(pp.queueOpts[rv.UID].preemption, api.Zero) {
			if pp.queueOpts[lv.UID].preemption.LessEqual(pp.queueOpts[rv.UID].preemption, api.Zero) {
				return -1
			}

			return 1
		}

		if pp.queueOpts[lv.UID].share == pp.queueOpts[rv.UID].share {
			return 0
		}

		if pp.queueOpts[lv.UID].share < pp.queueOpts[rv.UID].share {
			return -1
		}

		return 1
	})

	ssn.AddReclaimableFn(pp.Name(), func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		if !pp.enableTaskInProportion(reclaimer) {
			return reclaimees, util.Permit
		}

		var victims []*api.TaskInfo
		allocations := map[api.QueueID]*api.Resource{}

		for _, reclaimee := range reclaimees {
			job := ssn.Jobs[reclaimee.Job]
			attr := pp.queueOpts[job.Queue]

			if _, found := allocations[job.Queue]; !found {
				allocations[job.Queue] = attr.allocated.Clone()
			}
			allocated := allocations[job.Queue]
			if allocated.LessPartly(reclaimer.Resreq, api.Zero) {
				klog.V(3).Infof("Failed to allocate resource for Task <%s/%s> in Queue <%s>, not enough resource.",
					reclaimee.Namespace, reclaimee.Name, job.Queue)
				continue
			}

			if !allocated.LessEqual(attr.deserved, api.Zero) {
				allocated.Sub(reclaimee.Resreq)
				victims = append(victims, reclaimee)
			}
		}
		klog.V(4).Infof("Victims from proportion plugins are %+v", victims)
		return victims, util.Permit
	})

	ssn.AddOverusedFn(pp.Name(), func(obj interface{}) (bool, *api.OverusedInfo) {
		queue := obj.(*api.QueueInfo)
		attr := pp.queueOpts[queue.UID]

		overused := attr.deserved.LessEqual(attr.allocated, api.Zero)
		metrics.UpdateQueueOverused(attr.name, overused)
		var res *api.OverusedInfo
		if overused {
			res = &api.OverusedInfo{}
			res.Reason = string(vcv1beta1.NotEnoughResourcesInCluster)
			res.Message = "deserved is less than allocated"
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>, share <%v>",
				queue.Name, attr.deserved, attr.allocated, attr.share)
		}

		return overused, res
	})

	ssn.AddAllocatableFn(pp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		job, found := ssn.Jobs[candidate.Job]
		preemptable := false
		if !found {
			klog.V(3).Infof("Can't find job by id for task: <%v>", candidate)
		} else {
			preemptable = job.Preemptable
		}

		// Allow allocation over guaranteed resources for preemptable jobs
		if (preemptable && candidate.Resreq.LessEqual(pp.totalNotAllocatedResources, api.Zero)) || !pp.enableTaskInProportion(candidate) {
			return true
		}

		attr := pp.queueOpts[queue.UID]

		free, _ := attr.deserved.Diff(attr.allocated, api.Zero)
		allocatable := candidate.Resreq.LessEqual(free, api.Zero)
		if !allocatable {
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>; Candidate <%v>: resource request <%v>",
				queue.Name, attr.deserved, attr.allocated, candidate.Name, candidate.Resreq)
			ssn.SetJobPendingReason(job, pp.Name(), vcv1beta1.NotEnoughResourcesInCluster, "resreq is greater than deserved")
		}

		return allocatable
	})

	ssn.AddJobEnqueueableFn(pp.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)
		queueID := job.Queue
		attr := pp.queueOpts[queueID]
		queue := ssn.Queues[queueID]
		// If no capability is set, always enqueue the job.
		if attr.realCapability == nil {
			klog.V(4).Infof("Capability of queue <%s> was not set, allow job <%s/%s> to Inqueue.",
				queue.Name, job.Namespace, job.Name)
			return util.Permit
		}

		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("job %s MinResources is null.", job.Name)
			return util.Permit
		}
		minReq := job.GetMinResources()

		klog.V(5).Infof("job %s min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s> notAllocated: <%s>",
			job.Name, minReq.String(), queue.Name, attr.realCapability.String(), attr.allocated.String(), attr.inqueue.String(), attr.elastic.String(), pp.totalNotAllocatedResources.String())

		// Allow preemptable jobs to be inqueue over guaranteed resources
		if job.Preemptable && minReq.LessEqual(pp.totalNotAllocatedResources, api.Zero) {
			klog.V(4).Infof("job <%s> is preemptable, allow to Inqueue.", queue.Name)
			return util.Permit
		}

		// The queue resource quota limit has not reached
		r := minReq.Add(attr.allocated).Add(attr.inqueue).Sub(attr.elastic)
		rr := attr.realCapability.Clone()

		for name := range rr.ScalarResources {
			if _, ok := r.ScalarResources[name]; !ok {
				delete(rr.ScalarResources, name)
			}
		}

		inqueue := r.LessEqual(rr, api.Infinity)
		klog.V(5).Infof("job %s inqueue %v", job.Name, inqueue)
		if inqueue {
			attr.inqueue.Add(job.GetMinResources())
			return util.Permit
		}
		ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), "queue resource quota insufficient")
		ssn.SetJobPendingReason(job, pp.Name(), vcv1beta1.InternalError, "queue resource quota insufficient")
		return util.Reject
	})

	ssn.AddBestNodeFn(pp.Name(), func(task *api.TaskInfo, nodeScores map[float64][]*api.NodeInfo) *api.NodeInfo {
		if task.Preemptable {
			return nil
		}

		var scores []float64
		for score := range nodeScores {
			scores = append(scores, score)
		}

		sort.Slice(scores, func(i, j int) bool {
			return scores[i] > scores[j]
		})

		if len(scores) == 0 {
			return nil
		}

		maxScore := scores[0]
		if maxScore == 0 {
			maxScore = 1
		}

		for _, score := range scores {
			if 1.0-score/maxScore > pp.allowedDeltaFromBestNodeScore {
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

	// Register event handlers.
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			if !pp.enableTaskInProportion(event.Task) {
				return
			}

			job := ssn.Jobs[event.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Add(event.Task.Resreq)
			pp.calculateTotalResources()

			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Get(api.GPUResourceName), attr.allocated.Memory)
			gpu := pp.totalNotAllocatedResources.Get(api.GPUResourceName)
			metrics.UpdateTotalNotAllocated(pp.totalNotAllocatedResources.MilliCPU, gpu, pp.totalNotAllocatedResources.Memory)

			pp.updateShare(attr)

			klog.V(4).Infof("Proportion AllocateFunc: task <%v/%v>, resreq <%v>, share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)
		},
		DeallocateFunc: func(event *framework.Event) {
			if !pp.enableTaskInProportion(event.Task) {
				return
			}

			job := ssn.Jobs[event.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Sub(event.Task.Resreq)
			pp.calculateTotalResources()

			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Get(api.GPUResourceName), attr.allocated.Memory)
			gpu := pp.totalNotAllocatedResources.Get(api.GPUResourceName)
			metrics.UpdateTotalNotAllocated(pp.totalNotAllocatedResources.MilliCPU, gpu, pp.totalNotAllocatedResources.Memory)

			pp.updateShare(attr)

			klog.V(4).Infof("Proportion EvictFunc: task <%v/%v>, resreq <%v>, share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)
		},
	})
}

func (pp *proportionPlugin) OnSessionClose(ssn *framework.Session) {
	pp.totalResource = nil
	pp.totalGuarantee = nil
	pp.totalNotAllocatedResources = nil
	pp.queueOpts = nil
}

func (pp *proportionPlugin) updateShare(attr *queueAttr) {
	res := float64(0)

	// TODO(k82cn): how to handle fragment issues?
	for _, rn := range attr.deserved.ResourceNames() {
		share := helpers.Share(attr.allocated.Get(rn), attr.deserved.Get(rn))
		if share > res {
			res = share
		}
	}

	attr.share = res
	metrics.UpdateQueueShare(attr.name, attr.share)
}
