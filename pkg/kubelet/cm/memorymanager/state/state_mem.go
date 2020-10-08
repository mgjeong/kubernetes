/*
Copyright 2017 The Kubernetes Authors.

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
	"sync"

	"k8s.io/klog"
)

type stateMemory struct {
	sync.RWMutex
	assignments  ContainerMemoryAssignments
	machineState MemoryMap
}

var _ State = &stateMemory{}

// NewMemoryState creates new State for keeping track of cpu/pod assignment
func NewMemoryState() State {
	klog.Infof("[memorymanager] initializing new in-memory state store")
	return &stateMemory{
		assignments:  ContainerMemoryAssignments{},
		machineState: MemoryMap{},
	}
}

// GetMemoryState returns Memory Map that stored in State
func (s *stateMemory) GetMachineState() MemoryMap {
	s.RLock()
	defer s.RUnlock()

	return s.machineState.Clone()
}

// GetMemoryBlocks returns memory assignments of container
func (s *stateMemory) GetMemoryBlocks(podUID string, containerName string) []Block {
	s.RLock()
	defer s.RUnlock()

	if res, ok := s.assignments[podUID][containerName]; ok {
		return append([]Block{}, res...)
	}
	return nil
}

// GetMemoryAssignments returns ContainerMemoryAssignments
func (s *stateMemory) GetMemoryAssignments() ContainerMemoryAssignments {
	s.RLock()
	defer s.RUnlock()

	return s.assignments.Clone()
}

// SetMachineState stores MemoryMap in State
func (s *stateMemory) SetMachineState(memoryMap MemoryMap) {
	s.Lock()
	defer s.Unlock()

	s.machineState = memoryMap
	klog.Info("[memorymanager] updated machine memory state")
}

// SetMemoryBlocks stores memory assignments of container
func (s *stateMemory) SetMemoryBlocks(podUID string, containerName string, blocks []Block) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.assignments[podUID]; !ok {
		s.assignments[podUID] = map[string][]Block{}
	}

	s.assignments[podUID][containerName] = blocks
	klog.Infof("[memorymanager] updated memory state (pod: %s, container: %s)", podUID, containerName)
}

// SetMemoryAssignments sets ContainerMemoryAssignments by passed parameter
func (s *stateMemory) SetMemoryAssignments(assignments ContainerMemoryAssignments) {
	s.Lock()
	defer s.Unlock()

	s.assignments = assignments
}

// Delete deletes corresponding Block from ContainerMemoryAssignments
func (s *stateMemory) Delete(podUID string, containerName string) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.assignments[podUID]; !ok {
		return
	}

	if _, ok := s.assignments[podUID][containerName]; !ok {
		return
	}

	delete(s.assignments[podUID], containerName)
	if len(s.assignments[podUID]) == 0 {
		delete(s.assignments, podUID)
	}
	klog.V(2).Infof("[memorymanager] deleted memory assignment (pod: %s, container: %s)", podUID, containerName)
}

// ClearState clears machineState and ContainerMemoryAssignments
func (s *stateMemory) ClearState() {
	s.Lock()
	defer s.Unlock()

	s.machineState = MemoryMap{}
	s.assignments = make(ContainerMemoryAssignments)
	klog.V(2).Infof("[memorymanager] cleared state")
}
