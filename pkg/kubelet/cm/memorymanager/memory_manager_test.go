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

var (
	fakeMachineInfo = cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{
				Id:     0,
				Memory: 128 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						PageSize: pageSize1Gb,
						NumPages: 10,
					},
				},
			},
			{
				Id:     1,
				Memory: 128 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						PageSize: pageSize1Gb,
						NumPages: 10,
					},
				},
			},
		},
	}
	fakePolicySingleNUMA, _           = NewPolicySingleNUMA(&fakeMachineInfo, fakeReserved, topologymanager.NewFakeManager())
	fakeMachineStateWithoutAssignment = state.NodeMap{
		0: &state.NodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    127 * gb,
					Free:           127 * gb,
					Reserved:       0 * gb,
					SystemReserved: 1 * gb,
					TotalMemSize:   128 * gb,
				},
				hugepages1Gi: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
		1: &state.NodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    127 * gb,
					Free:           127 * gb,
					Reserved:       0 * gb,
					SystemReserved: 1 * gb,
					TotalMemSize:   128 * gb,
				},
				hugepages1Gi: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
	}
	fakeMachineStateWithOneAssignment = state.NodeMap{
		0: &state.NodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    127 * gb,
					Free:           126 * gb,
					Reserved:       1 * gb,
					SystemReserved: 1 * gb,
					TotalMemSize:   128 * gb,
				},
				hugepages1Gi: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
		1: &state.NodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    127 * gb,
					Free:           127 * gb,
					Reserved:       0 * gb,
					SystemReserved: 1 * gb,
					TotalMemSize:   128 * gb,
				},
				hugepages1Gi: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
	}
	fakeMachineStateWithTwoAssignment = state.NodeMap{
		0: &state.NodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    127 * gb,
					Free:           125 * gb,
					Reserved:       2 * gb,
					SystemReserved: 1 * gb,
					TotalMemSize:   128 * gb,
				},
				hugepages1Gi: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
		1: &state.NodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    127 * gb,
					Free:           127 * gb,
					Reserved:       0 * gb,
					SystemReserved: 1 * gb,
					TotalMemSize:   128 * gb,
				},
				hugepages1Gi: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
	}
	fakeAssignmentsEmpty        = state.ContainerMemoryAssignments{}
	fakeAssignmentsOneContainer = state.ContainerMemoryAssignments{
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
	}
	fakeAssignmentsTwoContainer = state.ContainerMemoryAssignments{
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
	}
	fakeReserved = systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
)

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
	testCases := []struct {
		description                string
		nodeAllocatableReservation v1.ResourceList
		preReservedMemory          map[int]map[v1.ResourceName]resource.Quantity
		expReserved                systemReservedMemory
		expErr                     error
	}{
		{
			description:                "Should return empty map when reservation is not done",
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			expReserved: systemReservedMemory{
				0: {},
				1: {},
			},
			expErr: nil,
		},
		{
			description:                "Should return error when Allocatable reservation is not equal pre reserved memory",
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			expReserved:                nil,
			expErr:                     fmt.Errorf("the total amount of memory of type \"memory\" is not equal to the value determined by Node Allocatable feature"),
		},
		{
			description:                "Should return error when Allocatable reservation is not equal pre reserved memory",
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			},
			expReserved: nil,
			expErr:      fmt.Errorf("the total amount of memory of type \"memory\" is not equal to the value determined by Node Allocatable feature"),
		},
		{
			description:                "Reserved should be equal to preReservedMemory",
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			},
			expReserved: fakeReserved,
			expErr:      nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			res, err := getSystemReservedMemory(&fakeMachineInfo, testCase.nodeAllocatableReservation, testCase.preReservedMemory)

			if !reflect.DeepEqual(res, testCase.expReserved) {
				t.Errorf("Memory Manager getReservedMemory() error, expected reserved %+v but got: %+v",
					testCase.expReserved, res)
			}
			if !reflect.DeepEqual(err, testCase.expErr) {
				t.Errorf("Memory Manager getReservedMemory() error, expected error %v but got: %v",
					testCase.expErr, err)
			}

		})
	}
}

func TestRemoveStaleState(t *testing.T) {
	testCases := []struct {
		description                   string
		policy                        Policy
		expError                      error
		assignments                   state.ContainerMemoryAssignments
		machineState                  state.NodeMap
		expContainerMemoryAssignments state.ContainerMemoryAssignments
		expMachineState               state.NodeMap
	}{
		{
			description: "Should fail - policy returns an error",
			policy: &mockPolicy{
				err: fmt.Errorf("Policy error"),
			},
			assignments:                   fakeAssignmentsTwoContainer,
			machineState:                  fakeMachineStateWithTwoAssignment,
			expContainerMemoryAssignments: fakeAssignmentsTwoContainer,
			expMachineState:               fakeMachineStateWithTwoAssignment,
		},
		{
			description:                   "Stale state succesfuly removed",
			policy:                        fakePolicySingleNUMA,
			assignments:                   fakeAssignmentsTwoContainer,
			machineState:                  fakeMachineStateWithTwoAssignment,
			expContainerMemoryAssignments: fakeAssignmentsEmpty,
			expMachineState:               fakeMachineStateWithoutAssignment,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       testCase.policy,
				state:        state.NewMemoryState(),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMemoryAssignments(testCase.assignments.Clone())
			mgr.state.SetMachineState(testCase.machineState.Clone())

			mgr.removeStaleState()

			if !reflect.DeepEqual(mgr.state.GetMemoryAssignments(), testCase.expContainerMemoryAssignments) {
				t.Errorf("Memory Manager removeStaleState() error, expected assignments %v but got: %v",
					testCase.expContainerMemoryAssignments, mgr.state.GetMemoryAssignments())
			}
		})

	}
}

func TestAddContainer(t *testing.T) {
	testCases := []struct {
		description        string
		updateErr          error
		policy             Policy
		assignments        state.ContainerMemoryAssignments
		machineState       state.NodeMap
		expAllocateErr     error
		expAddContainerErr error
		expMachineState    state.NodeMap
	}{
		{
			description:        "Correct allocation and adding container on NUMA 0.",
			updateErr:          nil,
			policy:             fakePolicySingleNUMA,
			assignments:        fakeAssignmentsEmpty,
			machineState:       fakeMachineStateWithoutAssignment,
			expAllocateErr:     nil,
			expAddContainerErr: nil,
			expMachineState:    fakeMachineStateWithOneAssignment,
		},
		{
			description:        "Shouldn't return any error when policy is None.",
			updateErr:          nil,
			policy:             NewPolicyNone(),
			assignments:        fakeAssignmentsEmpty,
			machineState:       state.NodeMap{},
			expAllocateErr:     nil,
			expAddContainerErr: nil,
			expMachineState:    state.NodeMap{},
		},
		{
			description: "Allocation should fail if policy returns an error.",
			updateErr:   nil,
			policy: &mockPolicy{
				err: fmt.Errorf("Fake reg error"),
			},
			assignments:        fakeAssignmentsEmpty,
			machineState:       fakeMachineStateWithoutAssignment,
			expAllocateErr:     fmt.Errorf("Fake reg error"),
			expAddContainerErr: nil,
			expMachineState:    fakeMachineStateWithoutAssignment,
		},
		{
			description:        "Adding container should fail (CRI error) but without an error",
			updateErr:          fmt.Errorf("Fake reg error"),
			policy:             fakePolicySingleNUMA,
			assignments:        fakeAssignmentsEmpty,
			machineState:       fakeMachineStateWithoutAssignment,
			expAllocateErr:     nil,
			expAddContainerErr: nil,
			expMachineState:    fakeMachineStateWithoutAssignment,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       testCase.policy,
				state:        state.NewMemoryState(),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: testCase.updateErr,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMachineState(testCase.machineState.Clone())
			mgr.state.SetMemoryAssignments(testCase.assignments.Clone())

			pod := getPod("fakePod1", "fakeContainer1", requirementsGuaranteed)
			container := &pod.Spec.Containers[0]
			err := mgr.Allocate(pod, container)
			if !reflect.DeepEqual(err, testCase.expAllocateErr) {
				t.Errorf("Memory Manager Allocate() error (%v), expected error: %v but got: %v",
					testCase.description, testCase.expAllocateErr, err)
			}
			err = mgr.AddContainer(pod, container, "fakeID")
			if !reflect.DeepEqual(err, testCase.expAddContainerErr) {
				t.Errorf("Memory Manager AddContainer() error (%v), expected error: %v but got: %v",
					testCase.description, testCase.expAddContainerErr, err)
			}
			if !reflect.DeepEqual(mgr.state.GetMachineState(), testCase.expMachineState) {
				//t.Errorf("Memory Manager MachineState error, expected free memory on NUMA 0: %+v Gb but got: %+v Gb, expected free hugepages on NUMA 0: %+v Gb but got %+v Gb",
				//testCase.expMachineState[0]["memory"].Free/gb, mgr.state.GetMachineState()[0]["memory"].Free/gb, testCase.expMachineState[0]["hugepages-1Gi"].Free/gb, mgr.state.GetMachineState()[0]["hugepages-1Gi"].Free/gb)
			}
		})
	}
}

func TestRemoveContainer(t *testing.T) {
	testCases := []struct {
		description                   string
		remContainerID                string
		policy                        Policy
		assignments                   state.ContainerMemoryAssignments
		machineState                  state.NodeMap
		expMachineState               state.NodeMap
		expContainerMemoryAssignments state.ContainerMemoryAssignments
		expError                      error
	}{
		{
			description:                   "Correct removing of a container",
			remContainerID:                "fakeID2",
			policy:                        fakePolicySingleNUMA,
			assignments:                   fakeAssignmentsTwoContainer,
			machineState:                  fakeMachineStateWithTwoAssignment,
			expError:                      nil,
			expMachineState:               fakeMachineStateWithOneAssignment,
			expContainerMemoryAssignments: fakeAssignmentsOneContainer,
		},
		{
			description:    "Should fail if policy returns an error",
			remContainerID: "fakeID1",
			policy: &mockPolicy{
				err: fmt.Errorf("Fake reg error"),
			},
			assignments:                   fakeAssignmentsTwoContainer,
			machineState:                  fakeMachineStateWithTwoAssignment,
			expError:                      fmt.Errorf("Fake reg error"),
			expMachineState:               fakeMachineStateWithTwoAssignment,
			expContainerMemoryAssignments: fakeAssignmentsTwoContainer,
		},
		{
			description:                   "Should do nothing if container not in containerMap",
			remContainerID:                "fakeID3",
			policy:                        fakePolicySingleNUMA,
			assignments:                   fakeAssignmentsTwoContainer,
			machineState:                  fakeMachineStateWithTwoAssignment,
			expError:                      nil,
			expMachineState:               fakeMachineStateWithTwoAssignment,
			expContainerMemoryAssignments: fakeAssignmentsTwoContainer,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			iniContainerMap := containermap.NewContainerMap()
			iniContainerMap.Add("fakePod1", "fakeContainer1", "fakeID1")
			iniContainerMap.Add("fakePod1", "fakeContainer2", "fakeID2")
			mgr := &manager{
				policy:       testCase.policy,
				state:        state.NewMemoryState(),
				containerMap: iniContainerMap,
				containerRuntime: mockRuntimeService{
					err: testCase.expError,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMemoryAssignments(testCase.assignments.Clone())
			mgr.state.SetMachineState(testCase.machineState.Clone())

			err := mgr.RemoveContainer(testCase.remContainerID)
			if !reflect.DeepEqual(err, testCase.expError) {
				t.Errorf("Memory Manager RemoveContainer() error (%v), expected error: %v but got: %v",
					testCase.description, testCase.expError, err)
			}
			if !reflect.DeepEqual(mgr.state.GetMemoryAssignments(), testCase.expContainerMemoryAssignments) {
				t.Errorf("Memory Manager RemoveContainer() inconsistent assignment, expected: %+v but got: %+v, start %+v",
					testCase.expContainerMemoryAssignments, mgr.state.GetMemoryAssignments(), testCase.expContainerMemoryAssignments)
			}

			if !reflect.DeepEqual(mgr.state.GetMachineState(), testCase.expMachineState) {
				//t.Errorf("Memory Manager MachineState error, expected state %+v but got: %+v",
				//testCase.expMachineState[0]["memory"], mgr.state.GetMachineState()[0]["memory"])
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	testCases := []struct {
		description                string
		policyName                 string
		machineInfo                *cadvisorapi.MachineInfo
		nodeAllocatableReservation v1.ResourceList
		preReservedMemory          map[int]map[v1.ResourceName]resource.Quantity
		affinity                   topologymanager.Store
		expErr                     error
	}{
		{
			description:                "Successfuly created Memory Manager instance",
			policyName:                 "single-numa",
			machineInfo:                &fakeMachineInfo,
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
			},
			affinity: topologymanager.NewFakeManager(),
			expErr:   nil,
		},
		{
			description:                "Should return an error where preReservedMemory is not correct",
			policyName:                 "single-numa",
			machineInfo:                &fakeMachineInfo,
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			preReservedMemory: map[int]map[v1.ResourceName]resource.Quantity{
				0: nodeResources{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				1: nodeResources{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			},
			affinity: topologymanager.NewFakeManager(),
			expErr:   fmt.Errorf("the total amount of memory of type \"memory\" is not equal to the value determined by Node Allocatable feature"),
		},
		{
			description:                "Should return an error when memory reserved for system is empty (preReservedMemory)",
			policyName:                 "single-numa",
			machineInfo:                &fakeMachineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			affinity:                   topologymanager.NewFakeManager(),
			expErr:                     fmt.Errorf("[memorymanager] you should specify the memory reserved for the system"),
		},
		{
			description:                "Should return an error where policy name is not correct",
			policyName:                 "dump-policy",
			machineInfo:                &fakeMachineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			affinity:                   topologymanager.NewFakeManager(),
			expErr:                     fmt.Errorf("unknown policy: \"dump-policy\""),
		},
		{
			description:                "Should return manager with none policy",
			policyName:                 "none",
			machineInfo:                &fakeMachineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			preReservedMemory:          map[int]map[v1.ResourceName]resource.Quantity{},
			affinity:                   topologymanager.NewFakeManager(),
			expErr:                     nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			stateFileDirectory, err := ioutil.TempDir("/tmp/", "memory_manager_tests")
			if err != nil {
				t.Errorf("Cannot create state file: %s", err.Error())
			}
			defer os.RemoveAll(stateFileDirectory)

			mgr, err := NewManager(testCase.policyName, testCase.machineInfo, testCase.nodeAllocatableReservation, testCase.preReservedMemory, stateFileDirectory, testCase.affinity)

			if !reflect.DeepEqual(err, testCase.expErr) {
				t.Errorf("Memory Manager NewManager() error, expected error '%v' but got: '%v'",
					testCase.expErr, err)
			}

			if testCase.expErr == nil {
				if mgr != nil {
					rawMgr := mgr.(*manager)
					if !reflect.DeepEqual(rawMgr.policy.Name(), testCase.policyName) {
						t.Errorf("Memory Manager NewManager() error, expected policyName %v but got: %v",
							testCase.policyName, rawMgr.policy.Name())
					}
					if testCase.policyName == "single-numa" {
						if !reflect.DeepEqual(rawMgr.policy.(*singleNUMAPolicy).systemReserved, fakeReserved) {
							t.Errorf("Memory Manager NewManager() error, expected systemReserved %+v but got: %+v",
								fakeReserved, rawMgr.policy.(*singleNUMAPolicy).systemReserved)
						}
					}
				} else {
					t.Errorf("Memory Manager NewManager undexpected error, manager=nil and it shouldn't be.")
				}

			}
		})
	}
}
