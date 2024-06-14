/*
Copyright 2022 The Kubernetes Authors.
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
	"testing"

	apiv1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func getWorkerAffinity() *apiv1.Affinity {
	return &apiv1.Affinity{
		PodAntiAffinity: &apiv1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "role",
								Operator: "In",
								Values:   []string{"worker"},
							},
						},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
		},
	}
}

type testParams struct {
	name             string
	pods             []*apiv1.Pod
	nodes            []*apiv1.Node
	pcs              []*schedulingv1.PriorityClass
	pgs              []*schedulingv1beta1.PodGroup
	qs               []*schedulingv1beta1.Queue
	expectedAffinity map[string]string // jobName -> NodeName
}

func paramsToCache(t *testing.T, params testParams) *cache.SchedulerCache {
	schedulerCache := cache.NewMockSchedulerCache()
	schedulerCache.StatusUpdater = &util.FakeStatusUpdater{}

	for _, node := range params.nodes {
		schedulerCache.Nodes[node.Name] = schedulingapi.NewNodeInfo(node)
	}
	for _, pod := range params.pods {
		schedulerCache.AddPod(pod)
	}
	for _, pc := range params.pcs {
		schedulerCache.PriorityClasses[pc.Name] = pc
	}
	for _, pg := range params.pgs {
		pg.Status = schedulingv1beta1.PodGroupStatus{
			Phase: schedulingv1beta1.PodGroupInqueue,
		}
		schedulerCache.AddPodGroupV1beta1(pg)
	}
	for _, q := range params.qs {
		schedulerCache.AddQueueV1beta1(q)
	}

	return schedulerCache
}

func TestGuarantee(t *testing.T) {
	framework.RegisterPluginBuilder(PluginName, New)
	options.ServerOpts = options.NewServerOption()
	defer framework.CleanupPluginBuilders()

	// Pending pods
	w1 := util.BuildPod("ns1", "worker-1", "", apiv1.PodPending, api.BuildResourceList("4", "4k"), "pg1", map[string]string{"role": "worker"}, map[string]string{})
	w2 := util.BuildPod("ns2", "worker-2", "", apiv1.PodPending, api.BuildResourceList("6", "6k"), "pg2", map[string]string{"role": "worker"}, map[string]string{})
	w3 := util.BuildPod("ns3", "worker-3", "", apiv1.PodPending, api.BuildResourceList("4", "4k"), "pg3", map[string]string{"role": "worker"}, map[string]string{})
	w1.Spec.Affinity = getWorkerAffinity()
	w2.Spec.Affinity = getWorkerAffinity()

	// nodes
	n1 := util.BuildNode("node1", api.BuildResourceList("8", "8k"), map[string]string{"selector": "worker"})
	n1.Status.Allocatable["pods"] = resource.MustParse("15")
	n1.Labels["kubernetes.io/hostname"] = "node1"

	// podgroup
	pg1 := &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "pg1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "q1",
			MinMember: int32(1),
		},
	}
	pg2 := &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns2",
			Name:      "pg2",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "q2",
			MinMember: int32(1),
		},
	}
	pg3 := &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns3",
			Name:      "pg3",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "q3",
			MinMember: int32(1),
		},
	}
	// queue
	queue1 := &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "q1",
		},
		Spec: schedulingv1beta1.QueueSpec{
			Weight: 1,
		},
	}
	// queue with guarantee
	queue2 := &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "q2",
		},
		Spec: schedulingv1beta1.QueueSpec{
			Weight: 1,
			Guarantee: schedulingv1beta1.Guarantee{
				Resource: api.BuildResourceList("6", "6k"),
			},
		},
	}
	queue3 := &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "q3",
		},
		Spec: schedulingv1beta1.QueueSpec{
			Weight: 1,
		},
	}

	// tests
	tests := []testParams{
		{
			name:  "remaining-sub-panic",
			pods:  []*apiv1.Pod{w1, w2, w3},
			nodes: []*apiv1.Node{n1},
			pgs:   []*schedulingv1beta1.PodGroup{pg1, pg2, pg3},
			qs:    []*schedulingv1beta1.Queue{queue1, queue2, queue3},
			expectedAffinity: map[string]string{
				"ns2-worker2": "node1",
			},
		},
	}

	for _, test := range tests {
		// initialize schedulerCache
		schedulerCache := paramsToCache(t, test)

		// session
		trueValue := true

		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:             PluginName,
						EnabledPredicate: &trueValue,
					},
				},
			},
		}, nil)

		allocator := allocate.New()
		allocator.Execute(ssn)

		for _, job := range ssn.Jobs {
			expectedNode, exist := test.expectedAffinity[job.Name]
			if !exist {
				// Doesn't have an affinity constraint for this job
				continue
			}

			// All tasks of the job must be on the expectedAffinity node
			for _, task := range job.Tasks {
				if task.Pod.Spec.NodeName != expectedNode {
					t.Logf("expected affinity <%s> for task <%s>", expectedNode, task.Pod.Spec.NodeName)
					t.Fail()
				}
			}
		}
	}
}
