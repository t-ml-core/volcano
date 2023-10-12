package queuemetrics

import (
	"k8s.io/klog"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "queue-metrics"

type queueMetricsPlugin struct {
}

// New return proportion action
func New(_ framework.Arguments) framework.Plugin {
	return &queueMetricsPlugin{}
}

func (pp *queueMetricsPlugin) Name() string {
	return PluginName
}

func (pp *queueMetricsPlugin) OnSessionOpen(ssn *framework.Session) {
	// Record metrics
	for _, queue := range ssn.Queues {
		metrics.UpdateQueuePodGroupInqueueCount(queue.Name, queue.Queue.Status.Inqueue)
		metrics.UpdateQueuePodGroupPendingCount(queue.Name, queue.Queue.Status.Pending)
		metrics.UpdateQueuePodGroupRunningCount(queue.Name, queue.Queue.Status.Running)
		metrics.UpdateQueuePodGroupUnknownCount(queue.Name, queue.Queue.Status.Unknown)
		metrics.UpdateQueuePodGroupCompletedCount(queue.Name, queue.Queue.Status.Completed)
	}
}

func (pp *queueMetricsPlugin) OnSessionClose(ssn *framework.Session) {
	// Write unsecduled jobs events, useful for alerts
	for _, job := range ssn.Jobs {
		jobQueue, exist := ssn.Queues[job.Queue]
		if !exist {
			klog.Errorf("Can't find queue for job <%s>, job id <%s>",
				job.Name, job.UID)
			continue
		}

		// Queue resources
		allocated := api.NewResource(jobQueue.Queue.Status.Allocated)
		guarantee := api.NewResource(jobQueue.Queue.Spec.Guarantee.Resource)

		jobRequests := job.TotalRequest.Clone()

		haveEnoughResourcesInGuarantee := allocated.Clone().Add(jobRequests).LessEqual(guarantee, api.Zero)

		// If the job doesn't have an inQueue status, then it was rejected by enqueue action
		// which is only possible if one of the enquable predicates from plugins returned false
		if (job.IsPending() || job.IsInqueue()) && haveEnoughResourcesInGuarantee {
			metrics.RegisterGuaranteeResourcesPenalty()
			klog.Errorf("Job <%s> for namespace <%s> isn't in running state, but have enought resources in quota. Guarantee <%s> job <%s> allocated <%s>",
				job.Name, job.Namespace, guarantee.String(), jobRequests.String(), allocated.String())
		}
	}
}
