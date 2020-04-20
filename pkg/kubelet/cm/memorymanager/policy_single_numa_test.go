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
	"reflect"
	"testing"

	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"

	cadvisorapi "github.com/google/cadvisor/info/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
)

const (
	mb           = 1024 * 1024
	gb           = mb * 1024
	pageSize1Gb  = 1048576
	hugepages1Gi = v1.ResourceName(v1.ResourceHugePagesPrefix + "1Gi")
)

var (
	requirementsGuaranteed = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
			hugepages1Gi:      resource.MustParse("1Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
			hugepages1Gi:      resource.MustParse("1Gi"),
		},
	}
	requirementsBurstable = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
			hugepages1Gi:      resource.MustParse("2Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
			hugepages1Gi:      resource.MustParse("1Gi"),
		},
	}
)

func isMemoryBlocksEqual(mb1, mb2 []state.Block) bool {
	if len(mb1) != len(mb2) {
		return false
	}

	copyMemoryBlocks := make([]state.Block, len(mb2))
	copy(copyMemoryBlocks, mb2)
	for _, block := range mb1 {
		for i, copyBlock := range copyMemoryBlocks {
			if reflect.DeepEqual(block, copyBlock) {
				// move the element that equal to the block to the end of the slice
				copyMemoryBlocks[i] = copyMemoryBlocks[len(copyMemoryBlocks)-1]

				// remove the last element from our slice
				copyMemoryBlocks = copyMemoryBlocks[:len(copyMemoryBlocks)-1]

				break
			}
		}
	}

	return len(copyMemoryBlocks) == 0
}

func isContainerMemoryAssignmentsEqual(cma1, cma2 state.ContainerMemoryAssignments) bool {
	if len(cma1) != len(cma2) {
		return false
	}

	for podUID, container := range cma1 {
		if _, ok := cma2[podUID]; !ok {
			return false
		}

		for containerName, memoryBlocks := range container {
			if _, ok := cma2[podUID][containerName]; !ok {
				return false
			}

			if !isMemoryBlocksEqual(memoryBlocks, cma2[podUID][containerName]) {
				return false
			}
		}
	}
	return true
}

type testSingleNUMAPolicy struct {
	description           string
	assignments           state.ContainerMemoryAssignments
	expectedAssignments   state.ContainerMemoryAssignments
	machineState          state.MemoryMap
	expectedMachineState  state.MemoryMap
	reserved              reservedMemory
	expectedError         error
	machineInfo           *cadvisorapi.MachineInfo
	pod                   *v1.Pod
	expectedTopologyHints map[string][]topologymanager.TopologyHint
}

func TestSingleNUMAPolicyStart(t *testing.T) {
	testCases := []testSingleNUMAPolicy{
		{
			description: "should fail, if machine state is empty, but it has memory assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state can not be empty, when it has memory assignments"),
		},
		{
			description:         "should fill the state with default values, when the state is empty",
			expectedAssignments: state.ContainerMemoryAssignments{},
			expectedMachineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
		},
		{
			description: "should fail when machine state does not have all NUMA nodes",
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
					{
						Id:     1,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state does not have NUMA node 1"),
		},
		{
			description: "should fail when machine state does not have memory resource",
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state does not have resource memory"),
		},
		{
			description: "should fail when machine state has wrong size of total memory",
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   1536 * mb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state has different size of the total memory"),
		},
		{
			description: "should fail when machine state has wrong size of system reserved memory",
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 1024,
						TotalMemSize:   2 * gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state has different size of the system reserved memory"),
		},
		{
			description: "should fail when machine state reserved memory is different from memory of all containers memory assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
					},
				},
			},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] memory reserved by containers different from the machine state reserved"),
		},
		{
			description: "should fail when machine state has wrong size of hugepages",
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 2,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state has different size of the total hugepages-1Gi"),
		},
		{
			description: "should fail when machine state has wrong size of system reserved hugepages",
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: gb,
						TotalMemSize:   2 * gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 2,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state has different size of the system reserved hugepages-1Gi"),
		},
		{
			description: "should fail when machine state reserved hugepages is different from hugepages of all containers memory assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
				"pod2": map[string][]state.Block{
					"container2": {
						{
							NUMAAffinity: 0,
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    4 * gb,
						Free:           gb,
						Reserved:       3 * gb,
						SystemReserved: 0,
						TotalMemSize:   4 * gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 4,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] hugepages-1Gi reserved by containers different from the machine state reserved"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			p := NewPolicySingleNUMA(testCase.machineInfo, testCase.reserved, topologymanager.NewFakeManager())
			s := state.NewMemoryState()
			s.SetMachineState(testCase.machineState)
			s.SetMemoryAssignments(testCase.assignments)

			err := p.Start(s)
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("The actual error: %v is different from the expected one: %v", err, testCase.expectedError)
			}

			if err != nil {
				return
			}

			assignments := s.GetMemoryAssignments()
			if !isContainerMemoryAssignmentsEqual(assignments, testCase.expectedAssignments) {
				t.Errorf("Actual assignments: %v is different from the expected one: %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !reflect.DeepEqual(machineState, testCase.expectedMachineState) {
				t.Errorf("The actual machine state: %v is different from the expected one: %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

func TestSingleNUMAPolicyAllocate(t *testing.T) {
	testCases := []testSingleNUMAPolicy{
		{
			description:         "should do nothing for non-guaranteed pods",
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			expectedMachineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPod("pod1", "container1", requirementsBurstable),
		},
		{
			description: "should do nothing once container already exists under the state file",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           512 * mb,
						Reserved:       1024 * mb,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			expectedMachineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           512 * mb,
						Reserved:       1024 * mb,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
		},
		{
			description: "should calculate default topology hint when no hint provided by topology manager",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: 0,
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			expectedMachineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           512 * mb,
						Reserved:       1024 * mb,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           0,
						Reserved:       gb,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			p := NewPolicySingleNUMA(testCase.machineInfo, testCase.reserved, topologymanager.NewFakeManager())
			s := state.NewMemoryState()
			s.SetMachineState(testCase.machineState)
			s.SetMemoryAssignments(testCase.assignments)

			err := p.Allocate(s, testCase.pod, &testCase.pod.Spec.Containers[0])
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("The actual error %v different from the expected one %v", err, testCase.expectedError)
			}

			if err != nil {
				return
			}

			assignments := s.GetMemoryAssignments()
			if !isContainerMemoryAssignmentsEqual(assignments, testCase.expectedAssignments) {
				t.Errorf("Actual assignments %v different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !reflect.DeepEqual(machineState, testCase.expectedMachineState) {
				t.Errorf("The actual machine state %v different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

func TestSingleNUMAPolicyRemoveContainer(t *testing.T) {
	testCases := []testSingleNUMAPolicy{
		{
			description:         "should do nothing when the container does not exist under the state",
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			expectedMachineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
		{
			description: "should delete the container assignment and update the machine state",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: 0,
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           512 * mb,
						Reserved:       1024 * mb,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           0,
						Reserved:       gb,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			expectedMachineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			reserved: reservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			p := NewPolicySingleNUMA(testCase.machineInfo, testCase.reserved, topologymanager.NewFakeManager())
			s := state.NewMemoryState()
			s.SetMachineState(testCase.machineState)
			s.SetMemoryAssignments(testCase.assignments)

			err := p.RemoveContainer(s, "pod1", "container1")
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("The actual error %v different from the expected one %v", err, testCase.expectedError)
			}

			if err != nil {
				return
			}

			assignments := s.GetMemoryAssignments()
			if !isContainerMemoryAssignmentsEqual(assignments, testCase.expectedAssignments) {
				t.Errorf("Actual assignments %v different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !reflect.DeepEqual(machineState, testCase.expectedMachineState) {
				t.Errorf("The actual machine state %v different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

func TestSingleNUMAPolicyGetTopologyHints(t *testing.T) {
	affinity0, err := bitmask.NewBitMask(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	affinity1, err := bitmask.NewBitMask(1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	testCases := []testSingleNUMAPolicy{
		{
			description: "should do provide topology hints for non-guaranteed pods",
			pod:         getPod("pod1", "container1", requirementsBurstable),
		},
		{
			description: "should provide topology hints based on the existent memory assignment",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: 0,
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: affinity0,
						Preferred:        true,
					},
				},
				string(hugepages1Gi): {
					{
						NUMANodeAffinity: affinity0,
						Preferred:        true,
					},
				},
			},
		},
		{
			description: "should calculate new topology hints, when the container does not exist under assignments",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: 0,
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: 0,
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.MemoryMap{
				0: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           512 * mb,
						Reserved:       1024 * mb,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           0,
						Reserved:       gb,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
				1: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    1536 * mb,
						Free:           1536 * mb,
						Reserved:       0,
						SystemReserved: 512 * mb,
						TotalMemSize:   2 * gb,
					},
					hugepages1Gi: {
						Allocatable:    gb,
						Free:           gb,
						Reserved:       0,
						SystemReserved: 0,
						TotalMemSize:   gb,
					},
				},
			},
			pod: getPod("pod2", "container2", requirementsGuaranteed),
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: affinity1,
						Preferred:        true,
					},
				},
				string(hugepages1Gi): {
					{
						NUMANodeAffinity: affinity1,
						Preferred:        true,
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			p := NewPolicySingleNUMA(testCase.machineInfo, testCase.reserved, topologymanager.NewFakeManager())
			s := state.NewMemoryState()
			s.SetMachineState(testCase.machineState)
			s.SetMemoryAssignments(testCase.assignments)

			topologyHints := p.GetTopologyHints(s, testCase.pod, &testCase.pod.Spec.Containers[0])
			if !reflect.DeepEqual(topologyHints, testCase.expectedTopologyHints) {
				t.Errorf("The actual topology hints: '%v' different from the expected one: '%v'", topologyHints, testCase.expectedTopologyHints)
			}
		})
	}
}
