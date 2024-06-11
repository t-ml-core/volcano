/*
Copyright 2022 The Volcano Authors.

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

package cdp

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// refer to issue https://github.com/volcano-sh/volcano/issues/2075,
	// plugin cdp means cooldown protection, related to elastic scheduler,
	// when we need to enable elastic training or serving,
	// preemptible job's pods can be preempted or back to running repeatedly,
	// if no cooldown protection set, these pods can be preempted again after they just started for a short time,
	// this may cause service stability dropped.
	// cdp plugin here is to ensure vcjob's pods cannot be preempted within cooldown protection conditions.
	// currently cdp plugin only support cooldown time protection.
	PluginName = "cdp"

	cooldownTimeForPreemptedJobOpt = "cooldown-time-for-preempted-job"
)

type CooldownProtectionPlugin struct {
	cooldownTimeForPreemptedJob time.Duration
}

// New return CooldownProtectionPlugin
func New(arguments framework.Arguments) framework.Plugin {
	plugin := &CooldownProtectionPlugin{cooldownTimeForPreemptedJob: 5 * time.Minute}
	if cooldownTimeForPreemptedJobI, ok := arguments[cooldownTimeForPreemptedJobOpt]; ok {
		if cooldownTimeForPreemptedJobS, ok := cooldownTimeForPreemptedJobI.(string); ok {
			cooldownTimeForPreemptedJob, err := time.ParseDuration(cooldownTimeForPreemptedJobS)
			if err != nil {
				klog.Warningf("invalid time duration %s=%s", cooldownTimeForPreemptedJobOpt, cooldownTimeForPreemptedJobS)
			} else {
				plugin.cooldownTimeForPreemptedJob = cooldownTimeForPreemptedJob
			}
		}
	}

	klog.V(5).Infof("parsed cdp args %s: %v",
		cooldownTimeForPreemptedJobOpt, plugin.cooldownTimeForPreemptedJob,
	)

	return plugin
}

// Name implements framework.Plugin
func (*CooldownProtectionPlugin) Name() string {
	return PluginName
}

func (sp *CooldownProtectionPlugin) podCooldownTime(pod *v1.Pod) (value time.Duration, enabled bool) {
	// check labels and annotations
	v, ok := pod.Labels[v1beta1.CooldownTime]
	if !ok {
		v, ok = pod.Annotations[v1beta1.CooldownTime]
		if !ok {
			return 0, false
		}
	}
	vi, err := time.ParseDuration(v)
	if err != nil {
		klog.Warningf("invalid time duration %s=%s", v1beta1.CooldownTime, v)
		return 0, false
	}
	return vi, true
}

// OnSessionOpen implements framework.Plugin
func (sp *CooldownProtectionPlugin) OnSessionOpen(ssn *framework.Session) {
	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		for _, preemptee := range preemptees {
			cooldownTime, enabled := sp.podCooldownTime(preemptee.Pod)
			if !enabled {
				victims = append(victims, preemptee)
				continue
			}
			pod := preemptee.Pod
			// find the time of pod really transform to running
			// only running pod check stable time, others all put into victims
			stableFiltered := false
			if pod.Status.Phase == v1.PodRunning {
				// ensure pod is running and have ready state
				for _, c := range pod.Status.Conditions {
					if c.Type == v1.PodScheduled && c.Status == v1.ConditionTrue {
						if c.LastTransitionTime.Add(cooldownTime).After(time.Now()) {
							stableFiltered = true
						}
						break
					}
				}
			}
			if !stableFiltered {
				victims = append(victims, preemptee)
			}
		}

		klog.V(4).Infof("Victims from cdp plugins are %+v", victims)
		return victims, util.Permit
	}

	klog.V(4).Info("plugin cdp session open")
	ssn.AddPreemptableFn(sp.Name(), preemptableFn)

	ssn.AddJobEnqueueableFn(sp.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)
		if job.PodGroup == nil {
			return util.Permit
		}

		pendingReasonInfo := job.PodGroup.Status.PendingReasonInfo
		if pendingReasonInfo.Reason != scheduling.JobPreempted {
			return util.Permit
		}

		if pendingReasonInfo.LastTransitionTime.Add(sp.cooldownTimeForPreemptedJob).After(time.Now()) {
			klog.V(3).Infof("did not enqueue job %s due to cdp plugin's decision", job.Name)
			return util.Reject
		}

		return util.Permit
	})
}

// OnSessionClose implements framework.Plugin
func (*CooldownProtectionPlugin) OnSessionClose(ssn *framework.Session) {}
