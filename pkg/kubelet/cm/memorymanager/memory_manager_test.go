/*
Copyright 2020 The Kubernetes Authors.

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

package memorymanager

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	cadvisorapi "github.com/google/cadvisor/info/v1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
)

const (
	hugepages2M = "hugepages-2Mi"
	hugepages1G = "hugepages-1Gi"
)

const policyTypeMock policyType = "mock"

type testMemoryManager struct {
	description                string
	policy                     Policy
	machineInfo                cadvisorapi.MachineInfo
	assignments                state.ContainerMemoryAssignments
	expectedAssignments        state.ContainerMemoryAssignments
	machineState               state.NodeMap
	expectedMachineState       state.NodeMap
	expectedError              error
	expectedAllocateError      error
	expectedAddContainerError  error
	updateError                error
	removeContainerID          string
	nodeAllocatableReservation v1.ResourceList
	policyName                 string
	affinity                   topologymanager.Store
	preReservedMemory          map[int]map[v1.ResourceName]resource.Quantity
	expectedHints              map[string][]topologymanager.TopologyHint
	expectedReserved           systemReservedMemory
	reserved                   systemReservedMemory
	podAllocate                *v1.Pod
	firstPod                   *v1.Pod
	activePods                 []*v1.Pod
}

func returnPolicyByName(testCase testMemoryManager) Policy {
	switch policyType(testCase.policyName) {
	case policyTypeMock:
		return &mockPolicy{
			err: fmt.Errorf("Fake reg error"),
		}
	case policyTypeStatic:
		policy, _ := NewPolicyStatic(&testCase.machineInfo, testCase.reserved, topologymanager.NewFakeManager())
		return policy
	case policyTypeNone:
		return NewPolicyNone()
	}
	return nil
}

type nodeResources map[v1.ResourceName]resource.Quantity

type mockPolicy struct {
	err error
}

func (p *mockPolicy) Name() string {
	return "mock"
}

func (p *mockPolicy) Start(s state.State) error {
	return p.err
}

func (p *mockPolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	return p.err
}

func (p *mockPolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	return p.err
}

func (p *mockPolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	return nil
}

type mockRuntimeService struct {
	err error
}

func (rt mockRuntimeService) UpdateContainerResources(id string, resources *runtimeapi.LinuxContainerResources) error {
	return rt.err
}

type mockPodStatusProvider struct {
	podStatus v1.PodStatus
	found     bool
}

func (psp mockPodStatusProvider) GetPodStatus(uid types.UID) (v1.PodStatus, bool) {
	return psp.podStatus, psp.found
}

func getPod(podUID string, containerName string, requirements *v1.ResourceRequirements) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(podUID),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      containerName,
					Resources: *requirements,
				},
			},
		},
	}
}

func TestValidatePreReservedMemory(t *testing.T) {
	const msgNotEqual = "the total amount of memory of type \"%s\" is not equal to the value determined by Node Allocatable feature"
	testCases := []struct {
		description                string
		nodeAllocatableReservation v1.ResourceList
		preReservedMemory          map[int]map[v1.ResourceName]resource.Quantity
		expectedError              string
	}{
		{
			"Node Allocatable not set, pre-reserved not set",
			v1.ResourceList{},
			map[int]map[v1.ResourceName]resource.Quantity{},
			"",
		},
		{
			"Node Allocatable set to zero, pre-reserved set to zero",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(0, resource.DecimalSI)},
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(0, resource.DecimalSI)},
			},
			"",
		},
		{
			"Node Allocatable not set (equal zero), pre-reserved set",
			v1.ResourceList{},
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI)},
			},
			fmt.Sprintf(msgNotEqual, v1.ResourceMemory),
		},
		{
			"Node Allocatable set, pre-reserved not set",
			v1.ResourceList{hugepages2M: *resource.NewQuantity(5, resource.DecimalSI)},
			map[int]map[v1.ResourceName]resource.Quantity{},
			fmt.Sprintf(msgNotEqual, hugepages2M),
		},
		{
			"Pre-reserved not equal to Node Allocatable",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI)},
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI)},
			},
			fmt.Sprintf(msgNotEqual, v1.ResourceMemory),
		},
		{
			"Pre-reserved total equal to Node Allocatable",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(17, resource.DecimalSI),
				hugepages2M: *resource.NewQuantity(77, resource.DecimalSI),
				hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(70, resource.DecimalSI),
					hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(7, resource.DecimalSI)},
			},
			"",
		},
		{
			"Pre-reserved total hugapages-2M not equal to Node Allocatable",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(17, resource.DecimalSI),
				hugepages2M: *resource.NewQuantity(14, resource.DecimalSI),
				hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(70, resource.DecimalSI),
					hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(7, resource.DecimalSI)},
			},
			fmt.Sprintf(msgNotEqual, hugepages2M),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := validatePreReservedMemory(tc.nodeAllocatableReservation, tc.preReservedMemory)
			if strings.TrimSpace(tc.expectedError) != "" {
				assert.Error(t, err)
				assert.Equal(t, err.Error(), tc.expectedError)
			}
		})
	}
}

func TestConvertPreReserved(t *testing.T) {
	machineInfo := cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{Id: 0},
			{Id: 1},
		},
	}

	testCases := []struct {
		description            string
		systemReserved         map[int]map[v1.ResourceName]resource.Quantity
		systemReservedExpected systemReservedMemory
		expectedError          string
	}{
		{
			"Empty",
			map[int]map[v1.ResourceName]resource.Quantity{},
			systemReservedMemory{
				0: map[v1.ResourceName]uint64{},
				1: map[v1.ResourceName]uint64{},
			},
			"",
		},
		{
			"Single NUMA node is pre-reserved",
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(70, resource.DecimalSI),
					hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
			},
			systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 12,
					hugepages2M:       70,
					hugepages1G:       13,
				},
				1: map[v1.ResourceName]uint64{},
			},
			"",
		},
		{
			"Both NUMA nodes are pre-reserved",
			map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(70, resource.DecimalSI),
					hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
					hugepages2M: *resource.NewQuantity(7, resource.DecimalSI)},
			},
			systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 12,
					hugepages2M:       70,
					hugepages1G:       13,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 5,
					hugepages2M:       7,
				},
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			reserved, _ := convertPreReserved(&machineInfo, tc.systemReserved)
			if !reflect.DeepEqual(reserved, tc.systemReservedExpected) {
				t.Errorf("got %v, expected %v", reserved, tc.systemReservedExpected)
			}
		})
	}
}

func TestGetSystemReservedMemory(t *testing.T) {
	machineInfo := returnMachineInfo()
	testCases := []testMemoryManager{
		{
			description:                "Should return empty map when reservation is not done",
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			expectedReserved: systemReservedMemory{
				0: {},
				1: {},
			},
			expectedError: nil,
			machineInfo:   machineInfo,
		},
		{
			description:                "Should return error when Allocatable reservation is not equal pre reserved memory",
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			},
			expectedReserved: nil,
			expectedError:    fmt.Errorf("the total amount of memory of type \"memory\" is not equal to the value determined by Node Allocatable feature"),
			machineInfo:      machineInfo,
		},
		{
			description:                "Reserved should be equal to preReservedMemory",
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			},
			expectedReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			expectedError: nil,
			machineInfo:   machineInfo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			res, err := getSystemReservedMemory(&testCase.machineInfo, testCase.nodeAllocatableReservation, testCase.preReservedMemory)

			if !reflect.DeepEqual(res, testCase.expectedReserved) {
				t.Errorf("Memory Manager getReservedMemory() error, expected reserved %+v, but got: %+v",
					testCase.expectedReserved, res)
			}
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("Memory Manager getReservedMemory() error, expected error %v, but got: %v",
					testCase.expectedError, err)
			}

		})
	}
}

func TestRemoveStaleState(t *testing.T) {
	machineInfo := returnMachineInfo()
	testCases := []testMemoryManager{
		{
			description: "Should fail - policy returns an error",
			policyName:  "mock",
			machineInfo: machineInfo,
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
		},
		{
			description: "Stale state succesfuly removed, without multi NUMA assignments",
			policyName:  "static",
			machineInfo: machineInfo,
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
		},
		{
			description: "Stale state succesfuly removed, with multi NUMA assignments",
			policyName:  "static",
			machineInfo: machineInfo,
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         12 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           0 * gb,
							Reserved:       9 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           5 * gb,
							Reserved:       4 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       returnPolicyByName(testCase),
				state:        state.NewMemoryState(),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMemoryAssignments(testCase.assignments)
			mgr.state.SetMachineState(testCase.machineState)

			mgr.removeStaleState()

			if !areContainerMemoryAssignmentsEqual(mgr.state.GetMemoryAssignments(), testCase.expectedAssignments) {
				t.Errorf("Memory Manager removeStaleState() error, expected assignments %v, but got: %v",
					testCase.expectedAssignments, mgr.state.GetMemoryAssignments())
			}
			if !areMachineStatesEqual(mgr.state.GetMachineState(), testCase.expectedMachineState) {
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", mgr.state.GetMachineState(), testCase.expectedMachineState)
			}
		})

	}
}

func TestAddContainer(t *testing.T) {
	machineInfo := returnMachineInfo()
	reserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	pod := getPod("fakePod1", "fakeContainer1", requirementsGuaranteed)
	testCases := []testMemoryManager{
		{
			description: "Correct allocation and adding container on NUMA 0",
			policyName:  "static",
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			updateError:               nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description:               "Shouldn't return any error when policy is set as None",
			updateError:               nil,
			policyName:                "none",
			machineInfo:               machineInfo,
			reserved:                  reserved,
			machineState:              state.NodeMap{},
			expectedMachineState:      state.NodeMap{},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description: "Allocation should fail if policy returns an error",
			updateError: nil,
			policyName:  "mock",
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     fmt.Errorf("Fake reg error"),
			expectedAddContainerError: nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description: "Adding container should fail (CRI error) but without an error",
			updateError: fmt.Errorf("Fake reg error"),
			policyName:  "static",
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description: "Correct allocation of container requiring amount of memory higher than capacity of one NUMA node",
			policyName:  "static",
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           0 * gb,
							Reserved:       9 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           1 * gb,
							Reserved:       4 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           6 * gb,
							Reserved:       3 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			podAllocate: getPod("fakePod1", "fakeContainer1", &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
			}),
			assignments: state.ContainerMemoryAssignments{},
			activePods:  nil,
		},
		{
			description: "Should fail if try to allocate container requiring amount of memory higher than capacity of one NUMA node but a small pod is already allocated",
			policyName:  "static",
			machineInfo: machineInfo,
			firstPod:    pod,
			reserved:    reserved,
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available"),
			expectedAddContainerError: nil,
			podAllocate: getPod("fakePod2", "fakeContainer2", &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
			}),
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						UID: types.UID("fakePod1"),
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:      "fakeContainer1",
								Resources: *requirementsGuaranteed,
							},
						},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       returnPolicyByName(testCase),
				state:        state.NewMemoryState(),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: testCase.updateError,
				},
				activePods:        func() []*v1.Pod { return testCase.activePods },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMachineState(testCase.machineState)
			mgr.state.SetMemoryAssignments(testCase.assignments)
			if testCase.firstPod != nil {
				mgr.containerMap.Add(testCase.firstPod.Name, testCase.firstPod.Spec.Containers[0].Name, "fakeID0")
			}
			pod := testCase.podAllocate
			container := &pod.Spec.Containers[0]
			err := mgr.Allocate(pod, container)
			if !reflect.DeepEqual(err, testCase.expectedAllocateError) {
				t.Errorf("Memory Manager Allocate() error (%v), expected error: %v, but got: %v",
					testCase.description, testCase.expectedAllocateError, err)
			}
			err = mgr.AddContainer(pod, container, "fakeID")
			if !reflect.DeepEqual(err, testCase.expectedAddContainerError) {
				t.Errorf("Memory Manager AddContainer() error (%v), expected error: %v, but got: %v",
					testCase.description, testCase.expectedAddContainerError, err)
			}

			if !areMachineStatesEqual(mgr.state.GetMachineState(), testCase.expectedMachineState) {
				t.Errorf("[test] %+v", mgr.state.GetMemoryAssignments())
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", mgr.state.GetMachineState(), testCase.expectedMachineState)
			}

		})
	}
}

func TestRemoveContainer(t *testing.T) {
	machineInfo := returnMachineInfo()
	reserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	testCases := []testMemoryManager{
		{
			description:       "Correct removing of a container",
			removeContainerID: "fakeID2",
			policyName:        "static",
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			description:       "Correct removing of a multi NUMA container",
			removeContainerID: "fakeID2",
			policyName:        "static",
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         12 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           0 * gb,
							Reserved:       9 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           5 * gb,
							Reserved:       4 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			description:       "Should fail if policy returns an error",
			removeContainerID: "fakeID1",
			policyName:        "mock",
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: fmt.Errorf("Fake reg error"),
		},
		{
			description:       "Should do nothing if container is not in containerMap",
			removeContainerID: "fakeID3",
			policyName:        "static",
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			iniContainerMap := containermap.NewContainerMap()
			iniContainerMap.Add("fakePod1", "fakeContainer1", "fakeID1")
			iniContainerMap.Add("fakePod1", "fakeContainer2", "fakeID2")
			mgr := &manager{
				policy:       returnPolicyByName(testCase),
				state:        state.NewMemoryState(),
				containerMap: iniContainerMap,
				containerRuntime: mockRuntimeService{
					err: testCase.expectedError,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMemoryAssignments(testCase.assignments)
			mgr.state.SetMachineState(testCase.machineState)

			err := mgr.RemoveContainer(testCase.removeContainerID)
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("Memory Manager RemoveContainer() error (%v), expected error: %v, but got: %v",
					testCase.description, testCase.expectedError, err)
			}

			if !areContainerMemoryAssignmentsEqual(mgr.state.GetMemoryAssignments(), testCase.expectedAssignments) {
				t.Fatalf("Memory Manager RemoveContainer() inconsistent assignment, expected: %+v, but got: %+v, start %+v",
					testCase.expectedAssignments, mgr.state.GetMemoryAssignments(), testCase.expectedAssignments)
			}

			if !areMachineStatesEqual(mgr.state.GetMachineState(), testCase.expectedMachineState) {
				t.Errorf("[test] %+v", mgr.state.GetMemoryAssignments())
				t.Errorf("[test] %+v, %+v", mgr.state.GetMachineState()[0].MemoryMap["memory"], mgr.state.GetMachineState()[1].MemoryMap["memory"])
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", mgr.state.GetMachineState(), testCase.expectedMachineState)
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	machineInfo := returnMachineInfo()
	expectedReserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	testCases := []testMemoryManager{
		{
			description:                "Successful creation of Memory Manager instance",
			policyName:                 "static",
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			},
			affinity:         topologymanager.NewFakeManager(),
			expectedError:    nil,
			expectedReserved: expectedReserved,
		},
		{
			description:                "Should return an error when preReservedMemory (configured with kubelet flag) does not comply with Node Allocatable feature values",
			policyName:                 "static",
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			},
			affinity:         topologymanager.NewFakeManager(),
			expectedError:    fmt.Errorf("the total amount of memory of type %q is not equal to the value determined by Node Allocatable feature", v1.ResourceMemory),
			expectedReserved: expectedReserved,
		},
		{
			description:                "Should return an error when memory reserved for system is empty (preReservedMemory)",
			policyName:                 "static",
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			affinity:                   topologymanager.NewFakeManager(),
			expectedError:              fmt.Errorf("[memorymanager] you should specify the system reserved memory"),
			expectedReserved:           expectedReserved,
		},
		{
			description:                "Should return an error when policy name is not correct",
			policyName:                 "fake",
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			affinity:                   topologymanager.NewFakeManager(),
			expectedError:              fmt.Errorf("unknown policy: \"fake\""),
			expectedReserved:           expectedReserved,
		},
		{
			description:                "Should create manager with \"none\" policy",
			policyName:                 "none",
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			affinity:                   topologymanager.NewFakeManager(),
			expectedError:              nil,
			expectedReserved:           expectedReserved,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			stateFileDirectory, err := ioutil.TempDir("/tmp/", "memory_manager_tests")
			if err != nil {
				t.Errorf("Cannot create state file: %s", err.Error())
			}
			defer os.RemoveAll(stateFileDirectory)

			mgr, err := NewManager(testCase.policyName, &testCase.machineInfo, testCase.nodeAllocatableReservation, testCase.preReservedMemory, stateFileDirectory, testCase.affinity)

			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("Could not create the Memory Manager. Expected error: '%v', but got: '%v'",
					testCase.expectedError, err)
			}

			if testCase.expectedError == nil {
				if mgr != nil {
					rawMgr := mgr.(*manager)
					if !reflect.DeepEqual(rawMgr.policy.Name(), testCase.policyName) {
						t.Errorf("Could not create the Memory Manager. Expected policy name: %v, but got: %v",
							testCase.policyName, rawMgr.policy.Name())
					}
					if testCase.policyName == "static" {
						if !reflect.DeepEqual(rawMgr.policy.(*staticPolicy).systemReserved, testCase.expectedReserved) {
							t.Errorf("Could not create the Memory Manager. Expected system reserved: %+v, but got: %+v",
								testCase.expectedReserved, rawMgr.policy.(*staticPolicy).systemReserved)
						}
					}
				} else {
					t.Errorf("Could not create the Memory Manager - manager is nil, but it should not be.")
				}

			}
		})
	}
}

func TestGetTopologyHints(t *testing.T) {
	testCases := []testMemoryManager{
		{
			description: "Successful hint generation",
			policyName:  "static",
			machineInfo: returnMachineInfo(),
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NodeMap{
				0: &state.NodeState{
					Nodes:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NodeState{
					Nodes:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
			expectedHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: newNUMAAffinity(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(0, 1),
						Preferred:        false,
					},
				},
				string(hugepages1Gi): {
					{
						NUMANodeAffinity: newNUMAAffinity(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(0, 1),
						Preferred:        false,
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       returnPolicyByName(testCase),
				state:        state.NewMemoryState(),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMachineState(testCase.machineState.Clone())
			mgr.state.SetMemoryAssignments(testCase.assignments.Clone())

			pod := getPod("fakePod1", "fakeContainer1", requirementsGuaranteed)
			container := &pod.Spec.Containers[0]
			hints := mgr.GetTopologyHints(pod, container)
			if !reflect.DeepEqual(hints, testCase.expectedHints) {
				t.Errorf("Hints were not generated correctly. Hints generated: %+v, hints expected: %+v",
					hints, testCase.expectedHints)
			}
		})
	}

}

func returnMachineInfo() cadvisorapi.MachineInfo {
	return cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{
				Id:     0,
				Memory: 10 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						PageSize: pageSize1Gb,
						NumPages: 5,
					},
				},
			},
			{
				Id:     1,
				Memory: 10 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						PageSize: pageSize1Gb,
						NumPages: 5,
					},
				},
			},
		},
	}
}
