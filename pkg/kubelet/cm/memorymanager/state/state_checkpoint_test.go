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

package state

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/containermap"
	testutil "k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state/testing"
)

// TODO: we should adapt other unittest

const testingCheckpoint = "memorymanager_checkpoint_test"

var testingDir = os.TempDir()

// AssertStateEqual marks provided test as failed if provided states differ
func AssertStateEqual(t *testing.T, restoredState, expectedState State) {
	expectedMachineState := restoredState.GetMachineState()
	restoredMachineState := restoredState.GetMachineState()

	if !reflect.DeepEqual(expectedMachineState, restoredMachineState) {
		t.Errorf("Expected MachineState does not equal to restored one")
	}

	expectedMemoryAssignments := expectedState.GetMemoryAssignments()
	restoredMemoryAssignments := restoredState.GetMemoryAssignments()
	if !reflect.DeepEqual(expectedMemoryAssignments, restoredMemoryAssignments) {
		t.Errorf("State memory assignments mismatch. Have %+v, want %+v", restoredMemoryAssignments, expectedMemoryAssignments)
	}
}

func TestCheckpointStateRestore(t *testing.T) {
	testCases := []struct {
		description       string
		checkpointContent string
		initialContainers containermap.ContainerMap
		expectedError     string
		expectedState     *stateMemory
	}{
		{
			"Restore non-existing checkpoint",
			"",
			containermap.ContainerMap{},
			"",
			&stateMemory{},
		},
		{
			"Restore default cpu set",
			`{
				"machineState":{"0":{"memory":{"total":2048,"systemReserved":512,"allocatable":1024,"reserved":512,"free":1024}}},
				"entries":{"pod":{"container1":[{"affinity":0,"type":"memory","size":512}]}},
				"checksum": 2301963941
			}`,
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerMemoryAssignments{
					"pod": map[string][]Block{
						"container1": []Block{
							{
								Affinity: 0,
								Type:     MemoryTypeRegular,
								Size:     512,
							},
						},
					},
				},
				machineState: MemoryMap{
					0: map[MemoryType]MemoryTable{
						MemoryTypeRegular: MemoryTable{
							Allocatable:    1024,
							Free:           1024,
							Reserved:       512,
							SystemReserved: 512,
							TotalMemSize:   2048,
						},
					},
				},
			},
		},
		// {
		// 	"Restore valid checkpoint",
		// 	`{
		// 		"policyName": "none",
		// 		"defaultCPUSet": "1-3",
		// 		"entries": {
		// 			"pod": {
		// 				"container1": "4-6",
		// 				"container2": "1-3"
		// 			}
		// 		},
		// 		"checksum": 3610638499
		// 	}`,
		// 	containermap.ContainerMap{},
		// 	"",
		// 	&stateMemory{
		// 		assignments: ContainerMemoryAssignments{
		// 			"pod": map[string][]Block{
		// 				"container1": []Block{
		// 					{
		// 						Affinity: 0,
		// 						Type:     MemoryTypeRegular,
		// 						Size:     1024,
		// 					},
		// 				},
		// 			},
		// 		},
		// 		machineState: MemoryMap{
		// 			0: map[MemoryType]MemoryTable{
		// 				MemoryTypeRegular: MemoryTable{
		// 					Allocatable:    1024,
		// 					Free:           1024,
		// 					Reserved:       512,
		// 					SystemReserved: 512,
		// 					TotalMemSize:   2048,
		// 				},
		// 			},
		// 		},
		// 	},
		// },
		// {
		// 	"Restore checkpoint with invalid checksum",
		// 	`{
		// 		"policyName": "none",
		// 		"defaultCPUSet": "4-6",
		// 		"entries": {},
		// 		"checksum": 1337
		// 	}`,
		// 	containermap.ContainerMap{},
		// 	"checkpoint is corrupted",
		// 	&stateMemory{},
		// },
		// {
		// 	"Restore checkpoint with invalid JSON",
		// 	`{`,
		// 	containermap.ContainerMap{},
		// 	"unexpected end of JSON input",
		// 	&stateMemory{},
		// },
		// {
		// 	"Restore checkpoint with unparsable assignment entry",
		// 	`{
		// 		"defaultCPUSet": "1-3",
		// 		"entries": {
		// 			"pod": {
		// 				"container1": "4-6",
		// 				"container2": "asd"
		// 			}
		// 		},
		// 		"checksum": 962272150
		// 	}`,
		// 	containermap.ContainerMap{},
		// 	`could not parse cpuset "asd" for container "container2" in pod "pod": strconv.Atoi: parsing "asd": invalid syntax`,
		// 	&stateMemory{},
		// },
	}

	// create checkpoint manager for testing
	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	if err != nil {
		t.Fatalf("could not create testing checkpoint manager: %v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// ensure there is no previous checkpoint
			cpm.RemoveCheckpoint(testingCheckpoint)

			// prepare checkpoint for testing
			if strings.TrimSpace(tc.checkpointContent) != "" {
				checkpoint := &testutil.MockCheckpoint{Content: tc.checkpointContent}
				if err := cpm.CreateCheckpoint(testingCheckpoint, checkpoint); err != nil {
					t.Fatalf("could not create testing checkpoint: %v", err)
				}
			}

			restoredState, err := NewCheckpointState(testingDir, testingCheckpoint, tc.initialContainers)
			if err != nil {
				if strings.TrimSpace(tc.expectedError) != "" {
					tc.expectedError = "could not restore state from checkpoint: " + tc.expectedError
					if strings.HasPrefix(err.Error(), tc.expectedError) {
						t.Logf("got expected error: %v", err)
						return
					}
				}
				t.Fatalf("unexpected error while creatng checkpointState: %v", err)
			}

			// compare state after restoration with the one expected
			AssertStateEqual(t, restoredState, tc.expectedState)
		})
	}
}

func TestCheckpointStateStore(t *testing.T) {
	testCases := []struct {
		description   string
		expectedState *stateMemory
	}{
		{
			"Store default cpu set",
			&stateMemory{
				assignments: ContainerMemoryAssignments{
					"pod": map[string][]Block{
						"container1": []Block{
							{
								Affinity: 0,
								Type:     MemoryTypeRegular,
								Size:     1024,
							},
						},
					},
				},
				machineState: MemoryMap{
					0: map[MemoryType]MemoryTable{
						MemoryTypeRegular: MemoryTable{
							Allocatable:    1024,
							Free:           1024,
							Reserved:       512,
							SystemReserved: 512,
							TotalMemSize:   2048,
						},
					},
				},
			},
		},
		{
			"Store assignments",
			&stateMemory{
				assignments: ContainerMemoryAssignments{
					"pod": map[string][]Block{
						"container1": []Block{
							{
								Affinity: 0,
								Type:     MemoryTypeRegular,
								Size:     1024,
							},
						},
					},
				},
				machineState: MemoryMap{
					0: map[MemoryType]MemoryTable{
						MemoryTypeRegular: MemoryTable{
							Allocatable:    1024,
							Free:           1024,
							Reserved:       512,
							SystemReserved: 512,
							TotalMemSize:   2048,
						},
					},
				},
			},
		},
	}

	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	if err != nil {
		t.Fatalf("could not create testing checkpoint manager: %v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// ensure there is no previous checkpoint
			cpm.RemoveCheckpoint(testingCheckpoint)

			cs1, err := NewCheckpointState(testingDir, testingCheckpoint, nil)
			if err != nil {
				t.Fatalf("could not create testing checkpointState instance: %v", err)
			}

			// set values of cs1 instance so they are stored in checkpoint and can be read by cs2
			cs1.SetMachineState(tc.expectedState.machineState)
			cs1.SetMemoryAssignments(tc.expectedState.assignments)

			// restore checkpoint with previously stored values
			// cs2, err := NewCheckpointState(testingDir, testingCheckpoint, nil)
			// if err != nil {
			// 	t.Fatalf("could not create testing checkpointState instance: %v", err)
			// }

			// AssertStateEqual(t, cs2, tc.expectedState)
		})
	}
}

// func TestCheckpointStateHelpers(t *testing.T) {
// 	testCases := []struct {
// 		description  string
// 		machineState MemoryMap
// 		assignments  ContainerMemoryAssignments
// 	}{
// 		{
// 			description: "One container",
// 			assignments: ContainerMemoryAssignments{
// 				"pod": map[string][]Block{
// 					"container1": []Block{
// 						{
// 							Affinity: 0,
// 							Type:     MemoryTypeRegular,
// 							Size:     1024,
// 						},
// 					},
// 				},
// 			},
// 			machineState: MemoryMap{
// 				0: map[MemoryType]*MemoryTable{
// 					MemoryTypeRegular: &MemoryTable{
// 						Allocatable:    1024,
// 						Free:           1024,
// 						Reserved:       512,
// 						SystemReserved: 512,
// 						TotalMemSize:   2048,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			description: "Two containers",
// 			assignments: ContainerMemoryAssignments{
// 				"pod": map[string][]Block{
// 					"container1": []Block{
// 						{
// 							Affinity: 0,
// 							Type:     MemoryTypeRegular,
// 							Size:     1024,
// 						},
// 					},
// 				},
// 			},
// 			machineState: MemoryMap{
// 				0: map[MemoryType]*MemoryTable{
// 					MemoryTypeRegular: &MemoryTable{
// 						Allocatable:    1024,
// 						Free:           1024,
// 						Reserved:       512,
// 						SystemReserved: 512,
// 						TotalMemSize:   2048,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			description: "Container without assigned cpus",
// 			assignments: ContainerMemoryAssignments{
// 				"pod": map[string][]Block{
// 					"container1": []Block{
// 						{
// 							Affinity: 0,
// 							Type:     MemoryTypeRegular,
// 							Size:     1024,
// 						},
// 					},
// 				},
// 			},
// 			machineState: MemoryMap{
// 				0: map[MemoryType]*MemoryTable{
// 					MemoryTypeRegular: &MemoryTable{
// 						Allocatable:    1024,
// 						Free:           1024,
// 						Reserved:       512,
// 						SystemReserved: 512,
// 						TotalMemSize:   2048,
// 					},
// 				},
// 			},
// 		},
// 	}

// 	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
// 	if err != nil {
// 		t.Fatalf("could not create testing checkpoint manager: %v", err)
// 	}

// 	for _, tc := range testCases {
// 		t.Run(tc.description, func(t *testing.T) {
// 			// ensure there is no previous checkpoint
// 			cpm.RemoveCheckpoint(testingCheckpoint)

// 			state, err := NewCheckpointState(testingDir, testingCheckpoint, nil)
// 			if err != nil {
// 				t.Fatalf("could not create testing checkpointState instance: %v", err)
// 			}
// 			state.SetMachineState(tc.machineState)

// 			for pod := range tc.assignments {
// 				for container, set := range tc.assignments[pod] {
// 					state.Set(pod, container, set)
// 					if cpus, _ := state.GetCPUSet(pod, container); !cpus.Equals(set) {
// 						t.Fatalf("state inconsistent, got %q instead of %q", set, cpus)
// 					}

// 					state.Delete(pod, container)
// 					if _, ok := state.GetCPUSet(pod, container); ok {
// 						t.Fatal("deleted container still existing in state")
// 					}
// 				}
// 			}
// 		})
// 	}
// }

// func TestCheckpointStateClear(t *testing.T) {
// 	testCases := []struct {
// 		description   string
// 		defaultCPUset cpuset.CPUSet
// 		assignments   map[string]map[string]cpuset.CPUSet
// 	}{
// 		{
// 			"Valid state",
// 			cpuset.NewCPUSet(1, 5, 10),
// 			map[string]map[string]cpuset.CPUSet{
// 				"pod": {
// 					"container1": cpuset.NewCPUSet(1, 4),
// 				},
// 			},
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(tc.description, func(t *testing.T) {
// 			state, err := NewCheckpointState(testingDir, testingCheckpoint, "none", nil)
// 			if err != nil {
// 				t.Fatalf("could not create testing checkpointState instance: %v", err)
// 			}

// 			state.SetDefaultCPUSet(tc.defaultCPUset)
// 			state.SetCPUAssignments(tc.assignments)

// 			state.ClearState()
// 			if !cpuset.NewCPUSet().Equals(state.GetDefaultCPUSet()) {
// 				t.Fatal("cleared state with non-empty default cpu set")
// 			}
// 			for pod := range tc.assignments {
// 				for container := range tc.assignments[pod] {
// 					if _, ok := state.GetCPUSet(pod, container); ok {
// 						t.Fatalf("container %q in pod %q with non-default cpu set in cleared state", container, pod)
// 					}
// 				}
// 			}
// 		})
// 	}
// }
