package queuemetrics

import (
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

	for _, job := range ssn.Jobs {
		for _, errors := range job.NodesFitErrors {
			metrics.UpdateNodeFitErrorsCount(errors.NodeCounts())
		}
	}
}

func (pp *queueMetricsPlugin) OnSessionClose(ssn *framework.Session) {
}
