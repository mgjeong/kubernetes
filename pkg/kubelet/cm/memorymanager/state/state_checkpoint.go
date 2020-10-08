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
	"fmt"
	"path"
	"sync"

	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/containermap"
)

var _ State = &stateCheckpoint{}

type stateCheckpoint struct {
	sync.RWMutex
	cache             State
	checkpointManager checkpointmanager.CheckpointManager
	checkpointName    string
	initialContainers containermap.ContainerMap
}

// NewCheckpointState creates new State for keeping track of memory/pod assignment with checkpoint backend
func NewCheckpointState(stateDir, checkpointName string, initialContainers containermap.ContainerMap) (State, error) {
	checkpointManager, err := checkpointmanager.NewCheckpointManager(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager: %v", err)
	}
	stateCheckpoint := &stateCheckpoint{
		cache:             NewMemoryState(),
		checkpointManager: checkpointManager,
		checkpointName:    checkpointName,
		initialContainers: initialContainers,
	}

	if err := stateCheckpoint.restoreState(); err != nil {
		//lint:ignore ST1005 user-facing error message
		return nil, fmt.Errorf("could not restore state from checkpoint: %v, please drain this node and delete the memory manager checkpoint file %q before restarting Kubelet",
			err, path.Join(stateDir, checkpointName))
	}

	return stateCheckpoint, nil
}

// restores state from a checkpoint and creates it if it doesn't exist
func (sc *stateCheckpoint) restoreState() error {
	sc.Lock()
	defer sc.Unlock()
	var err error

	checkpoint := NewMemoryManagerCheckpoint()
	if err = sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpoint); err != nil {
		if err == errors.ErrCheckpointNotFound {
			return sc.storeState()
		}
		return err
	}

	sc.cache.SetMachineState(checkpoint.MachineState)
	sc.cache.SetMemoryAssignments(checkpoint.Entries)

	klog.V(2).Info("[memorymanager] state checkpoint: restored state from checkpoint")

	return nil
}

// saves state to a checkpoint, caller is responsible for locking
func (sc *stateCheckpoint) storeState() error {
	checkpoint := NewMemoryManagerCheckpoint()
	checkpoint.MachineState = sc.cache.GetMachineState()
	checkpoint.Entries = sc.cache.GetMemoryAssignments()

	err := sc.checkpointManager.CreateCheckpoint(sc.checkpointName, checkpoint)
	if err != nil {
		klog.Errorf("[memorymanager] could not save checkpoint: %v", err)
		return err
	}
	return nil
}

// GetMemoryState returns Memory Map stored in the State
func (sc *stateCheckpoint) GetMachineState() MemoryMap {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMachineState()
}

// GetMemoryBlocks returns memory assignments of a container
func (sc *stateCheckpoint) GetMemoryBlocks(podUID string, containerName string) []Block {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMemoryBlocks(podUID, containerName)
}

// GetMemoryAssignments returns ContainerMemoryAssignments
func (sc *stateCheckpoint) GetMemoryAssignments() ContainerMemoryAssignments {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMemoryAssignments()
}

// SetMachineState stores MemoryMap in State
func (sc *stateCheckpoint) SetMachineState(memoryMap MemoryMap) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetMachineState(memoryMap)
	err := sc.storeState()
	if err != nil {
		klog.Warningf("store state to checkpoint error: %v", err)
	}
}

// SetMemoryBlocks stores memory assignments of container
func (sc *stateCheckpoint) SetMemoryBlocks(podUID string, containerName string, blocks []Block) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetMemoryBlocks(podUID, containerName, blocks)
	err := sc.storeState()
	if err != nil {
		klog.Warningf("store state to checkpoint error: %v", err)
	}
}

// SetMemoryAssignments sets ContainerMemoryAssignments by using the passed parameter
func (sc *stateCheckpoint) SetMemoryAssignments(assignments ContainerMemoryAssignments) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetMemoryAssignments(assignments)
	err := sc.storeState()
	if err != nil {
		klog.Warningf("store state to checkpoint error: %v", err)
	}
}

// Delete deletes corresponding Blocks from ContainerMemoryAssignments
func (sc *stateCheckpoint) Delete(podUID string, containerName string) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.Delete(podUID, containerName)
	err := sc.storeState()
	if err != nil {
		klog.Warningf("store state to checkpoint error: %v", err)
	}
}

// ClearState clears machineState and ContainerMemoryAssignments
func (sc *stateCheckpoint) ClearState() {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.ClearState()
	err := sc.storeState()
	if err != nil {
		klog.Warningf("store state to checkpoint error: %v", err)
	}
}
