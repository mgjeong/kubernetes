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

package state

import (
	v1 "k8s.io/api/core/v1"
)

// MemoryTable contains memory information
type MemoryTable struct {
	TotalMemSize   uint64 `json:"total"`
	SystemReserved uint64 `json:"systemReserved"`
	Allocatable    uint64 `json:"allocatable"`
	Reserved       uint64 `json:"reserved"`
	Free           uint64 `json:"free"`
}

// MemoryMap contains memory information for each NUMA node.
type MemoryMap map[int]map[v1.ResourceName]MemoryTable

// Clone returns a copy of MemoryMap
func (mm MemoryMap) Clone() MemoryMap {
	clone := make(MemoryMap)
	for node, memory := range mm {
		clone[node] = map[v1.ResourceName]MemoryTable{}
		for memoryType, memoryTable := range memory {
			clone[node][memoryType] = MemoryTable{
				Allocatable:    memoryTable.Allocatable,
				Free:           memoryTable.Free,
				Reserved:       memoryTable.Reserved,
				SystemReserved: memoryTable.SystemReserved,
				TotalMemSize:   memoryTable.TotalMemSize,
			}
		}
	}
	return clone
}

// Block is a data structure used to represent a certain amount of memory
type Block struct {
	NUMAAffinity int             `json:"numaAffinity"`
	Type         v1.ResourceName `json:"type"`
	Size         uint64          `json:"size"`
}

// ContainerMemoryAssignments stores memory assignments of containers
type ContainerMemoryAssignments map[string]map[string][]Block

// Clone returns a copy of ContainerMemoryAssignments
func (as ContainerMemoryAssignments) Clone() ContainerMemoryAssignments {
	clone := make(ContainerMemoryAssignments)
	for pod := range as {
		clone[pod] = make(map[string][]Block)
		for container, blocks := range as[pod] {
			clone[pod][container] = append([]Block{}, blocks...)
		}
	}
	return clone
}

// Reader interface used to read current memory/pod assignment state
type Reader interface {
	// GetMachineState returns Memory Map stored in the State
	GetMachineState() MemoryMap
	// GetMemoryBlocks returns memory assignments of a container
	GetMemoryBlocks(podUID string, containerName string) []Block
	// GetMemoryAssignments returns ContainerMemoryAssignments
	GetMemoryAssignments() ContainerMemoryAssignments
}

type writer interface {
	// SetMachineState stores MemoryMap in State
	SetMachineState(memoryMap MemoryMap)
	// SetMemoryBlocks stores memory assignments of a container
	SetMemoryBlocks(podUID string, containerName string, blocks []Block)
	// SetMemoryAssignments sets ContainerMemoryAssignments by using the passed parameter
	SetMemoryAssignments(assignments ContainerMemoryAssignments)
	// Delete deletes corresponding Blocks from ContainerMemoryAssignments
	Delete(podUID string, containerName string)
	// ClearState clears machineState and ContainerMemoryAssignments
	ClearState()
}

// State interface provides methods for tracking and setting memory/pod assignment
type State interface {
	Reader
	writer
}
