/*
 Copyright 2021 The Volcano Authors.

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

package allocate

import (
	"fmt"
	"sort"
	"time"

	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	vcv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (alloc *Action) Name() string {
	return "allocate"
}

func (alloc *Action) Initialize() {}

func (alloc *Action) Execute(ssn *framework.Session) {
	ssn.LastActionName = alloc.Name()
	klog.V(5).Infof("Enter Allocate ...")
	defer klog.V(5).Infof("Leaving Allocate ...")

	// the allocation for pod may have many stages
	// 1. pick a queue named Q (using ssn.QueueOrderFn)
	// 2. pick a job named J from Q (using ssn.JobOrderFn)
	// 3. pick a task T from J (using ssn.TaskOrderFn)
	// 4. use predicateFn to filter out node that T can not be allocated on.
	// 5. use ssn.NodeOrderFn to judge the best node and assign it to T

	// queues sort queues by QueueOrderFn.
	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	// jobsMap is used to find job with the highest priority in given queue.
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		// If not config enqueue action, change Pending pg into Inqueue statue to avoid blocking job scheduling.
		if conf.EnabledActionMap["enqueue"] {
			if job.IsPending() {
				klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: job status is pending.",
					job.Namespace, job.Name, job.Queue)
				continue
			}
		} else if job.IsPending() {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> status update from pending to inqueue, reason: no enqueue action is configured.",
				job.Namespace, job.Name, job.Queue)
			job.UpdateStatus(scheduling.PodGroupInqueue)
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		if _, found := ssn.Queues[job.Queue]; !found {
			klog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			ssn.SetJobPendingReason(job, "", vcv1beta1.InternalError, "can't find job's queue")
			continue
		}

		if _, found := jobsMap[job.Queue]; !found {
			jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			queues.Push(ssn.Queues[job.Queue])
		}

		klog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		jobsMap[job.Queue].Push(job)
	}

	klog.V(3).Infof("Try to allocate resource to %d Queues", len(jobsMap))

	pendingTasks := map[api.JobID]*util.PriorityQueue{}

	allNodes := ssn.NodeList
	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) ([]*api.Status, error) {
		// Check for Resource Predicate
		if ok, resources := task.InitResreq.LessEqualWithResourcesName(node.FutureIdle(), api.Zero); !ok {
			if task.Preemptable {
				return nil, api.NewFitError(task, node, api.WrapInsufficientResourceReason(resources))
			}

			if ok, resources = task.InitResreq.LessEqualWithResourcesName(node.IdleAfterPreempt(task, ssn.Preemptable), api.Zero); !ok {
				return nil, api.NewFitError(task, node, api.WrapInsufficientResourceReason(resources))
			}
		}

		var statusSets util.StatusSets
		statusSets, err := ssn.PredicateFn(task, node)
		if err != nil {
			return nil, api.NewFitError(task, node, err.Error())
		}

		if statusSets.ContainsUnschedulable() || statusSets.ContainsUnschedulableAndUnresolvable() ||
			statusSets.ContainsErrorSkipOrWait() {
			return nil, api.NewFitError(task, node, statusSets.Message())
		}
		return nil, nil
	}

	// To pick <namespace, queue> tuple for job, we choose to pick namespace firstly.
	// Because we believe that number of queues would less than namespaces in most case.
	// And, this action would make the resource usage among namespace balanced.
	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)
		isOverused, info := ssn.Overused(queue)
		if isOverused {
			klog.V(3).Infof("Queue <%s> is overused, ignore it", queue.Name)
			jobs := jobsMap[queue.UID]

			for !jobs.Empty() {
				job := jobs.Pop().(*api.JobInfo)
				ssn.SetJobPendingReason(job, info.Plugin, vcv1beta1.PendingReason(info.Reason), info.Message)
			}

			continue
		}

		klog.V(3).Infof("Try to allocate resource to Jobs in Queue <%s>", queue.Name)

		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			klog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		job := jobs.Pop().(*api.JobInfo)
		if _, found = pendingTasks[job.UID]; !found {
			tasks := util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				// Skip BestEffort task in 'allocate' action.
				if task.Resreq.IsEmpty() {
					klog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
						task.Namespace, task.Name)
					continue
				}

				tasks.Push(task)
			}
			pendingTasks[job.UID] = tasks
		}
		tasks := pendingTasks[job.UID]

		// Added Queue back until no job in Namespace.
		queues.Push(queue)

		if tasks.Empty() {
			continue
		}

		klog.V(3).Infof("Try to allocate resource to %d tasks of Job <%v/%v>",
			tasks.Len(), job.Namespace, job.Name)

		stmt := framework.NewStatement(ssn)
		ph := util.NewPredicateHelper()
		for !tasks.Empty() {
			task := tasks.Pop().(*api.TaskInfo)

			if !ssn.Allocatable(queue, task) {
				// NOTE: the plugin whose allocatable function returned false have to set pending reason
				klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
				continue
			}

			klog.V(3).Infof("There are <%d> nodes for Job <%v/%v>", len(ssn.Nodes), job.Namespace, job.Name)

			if err := ssn.PrePredicateFn(task); err != nil {
				klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
				fitErrors := api.NewFitErrors()
				for _, ni := range allNodes {
					fitErrors.SetNodeError(ni.Name, err)
				}
				job.NodesFitErrors[task.UID] = fitErrors
				ssn.SetJobPendingReason(job, "", vcv1beta1.InternalError, fmt.Sprintf("can't fit job to node: %v", fitErrors))
				break
			}

			predicateNodes, fitErrors := ph.PredicateNodes(task, allNodes, predicateFn, true)
			if len(predicateNodes) == 0 {
				job.NodesFitErrors[task.UID] = fitErrors
				ssn.SetJobPendingReason(job, "", vcv1beta1.NotEnoughResourcesInCluster, "can't find node for the task")
				break
			}

			nodeScores := util.PrioritizeNodes(task, predicateNodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)
			var predicateNodeInfo []nodeInfo
			for score, nodes := range nodeScores {
				for _, node := range nodes {
					predicateNodeInfo = append(predicateNodeInfo, nodeInfo{
						NodeName: node.Name,
						Score:    score,
					})
				}
			}
			sort.Slice(predicateNodeInfo, func(i, j int) bool {
				return predicateNodeInfo[i].Score > predicateNodeInfo[j].Score
			})

			klog.V(3).Infof("predicated nodes %v for task %s/%s", predicateNodeInfo, task.Namespace, task.Name)

			bestNode := ssn.BestNodeFn(task, nodeScores)
			if bestNode == nil {
				bestNode = util.SelectBestNode(nodeScores)
			}

			// Allocate idle resource to the task.
			if task.InitResreq.LessEqual(bestNode.Idle, api.Zero) {
				klog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
					task.Namespace, task.Name, bestNode.Name)
				if err := stmt.Allocate(task, bestNode); err != nil {
					klog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
						task.UID, bestNode.Name, ssn.UID, err)
					ssn.SetJobPendingReason(job, "", vcv1beta1.InternalError, fmt.Sprintf("failed to bind Task on the node: %v", err))
				} else {
					metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
					metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
				}
			} else {
				klog.V(3).Infof("Predicates failed in allocate for task <%s/%s> on node <%s> with limited resources",
					task.Namespace, task.Name, bestNode.Name)

				// Allocate releasing resource to the task if any.
				if task.InitResreq.LessEqual(bestNode.FutureIdle(), api.Zero) {
					klog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
						task.Namespace, task.Name, bestNode.Name, task.InitResreq, bestNode.Releasing)
					if err := stmt.Pipeline(task, bestNode.Name); err != nil {
						klog.Errorf("Failed to pipeline Task %v on %v in Session %v for %v.",
							task.UID, bestNode.Name, ssn.UID, err)
					} else {
						metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
						metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
					}
				} else if !task.Preemptable && task.InitResreq.LessEqual(bestNode.IdleAfterPreempt(task, ssn.Preemptable), api.Zero) {
					ssn.SetJobPendingReason(job, "", vcv1beta1.InternalError,
						fmt.Sprintf("the resource on node %s will appear only after preemption", bestNode.Name))
				}
			}

			if ssn.JobReady(job) && !tasks.Empty() {
				jobs.Push(job)
				break
			}
		}

		if ssn.JobReady(job) {
			stmt.Commit()
		} else {
			if !ssn.JobPipelined(job) {
				stmt.Discard()
			}
		}
	}
}

func (alloc *Action) UnInitialize() {}

type nodeInfo struct {
	NodeName string
	Score    float64
}
