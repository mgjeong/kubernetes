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
	"sort"

	cadvisorapi "github.com/google/cadvisor/info/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
	corehelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
)

const policyTypeSingleNUMA policyType = "single-numa"

type systemReservedMemory map[int]map[v1.ResourceName]uint64

// SingleNUMAPolicy is implementation of the policy interface for the single NUMA policy
type singleNUMAPolicy struct {
	// crossNUMAGroups contains groups of NUMA node that can be used for cross NUMA memory allocations
	crossNUMAGroups [][]int
	// machineInfo contains machine memory related information
	machineInfo *cadvisorapi.MachineInfo
	// reserved contains memory that reserved for kube
	systemReserved systemReservedMemory
	// topology manager reference to get container Topology affinity
	affinity topologymanager.Store
}

var _ Policy = &singleNUMAPolicy{}

// NewPolicySingleNUMA returns new single NUMA policy instance
func NewPolicySingleNUMA(machineInfo *cadvisorapi.MachineInfo, reserved systemReservedMemory, affinity topologymanager.Store) Policy {
	// TODO: check if we have enough reserved memory for the system
	//for _, node := range reserved {
	//	if node[v1.ResourceMemory] == 0 {
	//
	//	}
	//}
	return &singleNUMAPolicy{
		machineInfo:    machineInfo,
		systemReserved: reserved,
		affinity:       affinity,
	}
}

func (p *singleNUMAPolicy) Name() string {
	return string(policyTypeSingleNUMA)
}

func (p *singleNUMAPolicy) Start(s state.State) error {
	if err := p.validateState(s); err != nil {
		klog.Errorf("[memorymanager] Invalid state: %v, please drain node and remove policy state file", err)
		return err
	}
	return nil
}

// Allocate call is idempotent
func (p *singleNUMAPolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	// allocate the memory only for guaranteed pods
	if v1qos.GetPodQOS(pod) != v1.PodQOSGuaranteed {
		return nil
	}

	klog.Infof("[memorymanager] Allocate (pod: %s, container: %s)", pod.Name, container.Name)
	if blocks := s.GetMemoryBlocks(string(pod.UID), container.Name); blocks != nil {
		klog.Infof("[memorymanager] Container already present in state, skipping (pod: %s, container: %s)", pod.Name, container.Name)
		return nil
	}

	// Call Topology Manager to get the aligned affinity across all hint providers.
	hint := p.affinity.GetAffinity(string(pod.UID), container.Name)
	klog.Infof("[memorymanager] Pod %v, Container %v Topology Affinity is: %v", pod.UID, container.Name, hint)

	var affinityBits []int
	// it can happen, when the best hint includes all NUMA nodes and memory manager enabled, it can happen
	// only under the topology manager best-effort or none policy
	if hint.NUMANodeAffinity == nil {
		defaultAffinity, err := p.getDefaultNUMAAffinity(s, container, hint.Preferred)
		if err != nil {
			return err
		}
		affinityBits = defaultAffinity.GetBits()
	} else {
		affinityBits = hint.NUMANodeAffinity.GetBits()
	}

	machineState := s.GetMachineState()

	var containerBlocks []state.Block
	for resourceName, q := range container.Resources.Requests {
		if resourceName != v1.ResourceMemory && !corehelper.IsHugePageResourceName(resourceName) {
			continue
		}

		size, succeed := q.AsInt64()
		if !succeed {
			return fmt.Errorf("[memorymanager] failed to represent quantity as int64")
		}

		// update the machine state
		nodeResourceMemoryState := machineState[affinityBits[0]].MemoryMap[resourceName]
		nodeResourceMemoryState.Reserved += uint64(size)
		nodeResourceMemoryState.Free -= uint64(size)

		// update memory blocks
		containerBlocks = append(containerBlocks, state.Block{
			NUMAAffinity: affinityBits,
			Size:         uint64(size),
			Type:         resourceName,
		})
	}

	s.SetMachineState(machineState)
	s.SetMemoryBlocks(string(pod.UID), container.Name, containerBlocks)

	return nil
}

// RemoveContainer call is idempotent
func (p *singleNUMAPolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	klog.Infof("[memorymanager] RemoveContainer (pod: %s, container: %s)", podUID, containerName)
	blocks := s.GetMemoryBlocks(podUID, containerName)
	if blocks == nil {
		return nil
	}

	s.Delete(podUID, containerName)

	// Mutate machine memory state to update free and reserved memory
	machineState := s.GetMachineState()
	for _, b := range blocks {
		for _, nodeId := range b.NUMAAffinity {
			nodeResourceMemoryState := machineState[nodeId].MemoryMap[b.Type]
			nodeResourceMemoryState.Free += b.Size
			nodeResourceMemoryState.Reserved -= b.Size
		}
	}
	s.SetMachineState(machineState)

	return nil
}

// GetTopologyHints implements the topologymanager.HintProvider Interface
// and is consulted to achieve NUMA aware resource alignment among this
// and other resource controllers.
func (p *singleNUMAPolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	if v1qos.GetPodQOS(pod) != v1.PodQOSGuaranteed {
		return nil
	}

	requestedResources, err := getRequestedResources(container)
	if err != nil {
		klog.Error(err.Error())
		return nil
	}

	hints := map[string][]topologymanager.TopologyHint{}
	for resourceName := range requestedResources {
		hints[string(resourceName)] = []topologymanager.TopologyHint{}
	}

	containerBlocks := s.GetMemoryBlocks(string(pod.UID), container.Name)
	// Short circuit to regenerate the same hints if there are already
	// memory allocated for the container. This might happen after a
	// kubelet restart, for example.
	if containerBlocks != nil {
		if len(containerBlocks) != len(requestedResources) {
			klog.Errorf("[memorymanager] The number of requested resources of the container %s differs from the number of memory blocks", container.Name)
			return nil
		}

		for _, b := range containerBlocks {
			if _, ok := requestedResources[b.Type]; !ok {
				klog.Errorf("[memorymanager] Container %s requested resources do not have resource of type %s", container.Name, b.Type)
				return nil
			}

			if b.Size != requestedResources[b.Type] {
				klog.Errorf("[memorymanager] Memory %s already allocated to (pod %v, container %v) with different number than request: requested: %d, allocated: %d", b.Type, pod.UID, container.Name, requestedResources[b.Type], b.Size)
				return nil
			}

			containerNUMAAffinity, err := bitmask.NewBitMask(b.NUMAAffinity...)
			if err != nil {
				klog.Errorf("[memorymanager] failed to generate NUMA bitmask: %v", err)
				return nil
			}

			klog.Infof("[memorymanager] Regenerating TopologyHints, %s was already allocated to (pod %v, container %v)", b.Type, pod.UID, container.Name)
			hints[string(b.Type)] = append(hints[string(b.Type)], topologymanager.TopologyHint{
				NUMANodeAffinity: containerNUMAAffinity,
				Preferred:        true,
			})
		}
		return hints
	}

	return p.calculateHints(s, requestedResources)
}

func getRequestedResources(container *v1.Container) (map[v1.ResourceName]uint64, error) {
	requestedResources := map[v1.ResourceName]uint64{}
	for resourceName, quantity := range container.Resources.Requests {
		if resourceName != v1.ResourceMemory && !corehelper.IsHugePageResourceName(resourceName) {
			continue
		}
		requestedSize, succeed := quantity.AsInt64()
		if !succeed {
			return nil, fmt.Errorf("[memorymanager] failed to represent quantity as int64")
		}
		requestedResources[resourceName] = uint64(requestedSize)
	}
	return requestedResources, nil
}

func (p *singleNUMAPolicy) calculateHints(s state.State, requestedResources map[v1.ResourceName]uint64) map[string][]topologymanager.TopologyHint {
	hints := map[string][]topologymanager.TopologyHint{}
	for resourceName := range requestedResources {
		hints[string(resourceName)] = []topologymanager.TopologyHint{}
	}

	machineState := s.GetMachineState()
	var numaNodes []int
	for n := range machineState {
		numaNodes = append(numaNodes, n)
	}

	bitmask.IterateBitMasks(numaNodes, func(mask bitmask.BitMask) {
		maskBits := mask.GetBits()
		singleNUMAHint := len(maskBits) == 1

		// the node already in group with another node, it can not be used for the single NUMA node hint
		if singleNUMAHint && len(machineState[maskBits[0]].Nodes) != 0 {
			return
		}

		totalFreeSize := map[v1.ResourceName]uint64{}
		// calculate total free memory for the node mask
		for _, nodeId := range maskBits {
			// the node already used for memory assignment
			if !singleNUMAHint && machineState[nodeId].NumberOfAssignments != 0 {
				// the node used for the single NUMA memory allocation, it can be used for cross NUMA hint
				if len(machineState[nodeId].Nodes) == 0 {
					return
				}

				nodeGroup := []int{nodeId}
				nodeGroup = append(nodeGroup, machineState[nodeId].Nodes...)

				// the node already used with different group of nodes, it can not be use with in the current hint
				if !areGroupsEqual(nodeGroup, maskBits) {
					return
				}
			}

			for resourceName := range requestedResources {
				if _, ok := totalFreeSize[resourceName]; !ok {
					totalFreeSize[resourceName] = 0
				}
				totalFreeSize[resourceName] += machineState[nodeId].MemoryMap[resourceName].Free
			}
		}

		// verify that for all memory types the node mask has enough resources
		for resourceName, requestedSize := range requestedResources {
			if totalFreeSize[resourceName] < requestedSize {
				return
			}
		}

		preferred := p.isHintPreferred(maskBits)
		// add the node mask as topology hint for all memory types
		for resourceName := range requestedResources {
			hints[string(resourceName)] = append(hints[string(resourceName)], topologymanager.TopologyHint{
				NUMANodeAffinity: mask,
				Preferred:        preferred,
			})
		}
	})

	return hints
}

func (p *singleNUMAPolicy) isHintPreferred(maskBits []int) bool {
	// check if the mask is for the single NUMA node hint
	if len(maskBits) == 1 {
		// the node should be used only for cross NUMA memory allocation
		return !p.isCrossNUMANode(maskBits[0])
	}

	return p.isCrossNUMAGroup(maskBits)
}

func (p *singleNUMAPolicy) isCrossNUMAGroup(maskBits []int) bool {
	for _, group := range p.crossNUMAGroups {
		if areGroupsEqual(group, maskBits) {
			return true
		}
	}
	return false
}

func areGroupsEqual(group1, group2 []int) bool {
	sort.Ints(group1)
	sort.Ints(group2)

	if len(group1) != len(group2) {
		return false
	}

	for i, elm := range group1 {
		if group2[i] != elm {
			return false
		}
	}
	return true
}

func (p *singleNUMAPolicy) isCrossNUMANode(nodeId int) bool {
	for _, group := range p.crossNUMAGroups {
		for _, groupNode := range group {
			if nodeId == groupNode {
				return true
			}
		}
	}
	return false
}

func (p *singleNUMAPolicy) validateState(s state.State) error {
	machineState := s.GetMachineState()
	memoryAssignments := s.GetMemoryAssignments()

	if len(machineState) == 0 {
		// Machine state cannot be empty when assignments exist
		if len(memoryAssignments) != 0 {
			return fmt.Errorf("[memorymanager] machine state can not be empty when it has memory assignments")
		}
		for _, node := range p.machineInfo.Topology {
			// fill memory table with regular memory values
			allocatable := node.Memory - p.systemReserved[node.Id][v1.ResourceMemory]
			machineState[node.Id] = &state.NodeState{
				NumberOfAssignments: 0,
				MemoryMap: map[v1.ResourceName]*state.MemoryTable{
					v1.ResourceMemory: {
						Allocatable:    allocatable,
						Free:           allocatable,
						Reserved:       0,
						SystemReserved: p.systemReserved[node.Id][v1.ResourceMemory],
						TotalMemSize:   node.Memory,
					},
				},
				Nodes: []int{},
			}

			// fill memory table with huge pages values
			for _, hugepage := range node.HugePages {
				hugepageQuantity := resource.NewQuantity(int64(hugepage.PageSize)*1024, resource.BinarySI)
				resourceName := corehelper.HugePageResourceName(*hugepageQuantity)
				// TODO: once it will be possible to reserve huge pages for system usage, we should update values
				machineState[node.Id].MemoryMap[resourceName] = &state.MemoryTable{
					TotalMemSize:   hugepage.NumPages * hugepage.PageSize * 1024,
					SystemReserved: 0,
					Allocatable:    hugepage.NumPages * hugepage.PageSize * 1024,
					Reserved:       0,
					Free:           hugepage.NumPages * hugepage.PageSize * 1024,
				}
			}
		}
		s.SetMachineState(machineState)
		return nil
	}

	// calculate all memory assigned to containers
	assignmentsMemory := map[int]map[v1.ResourceName]uint64{}
	for _, container := range memoryAssignments {
		for _, blocks := range container {
			for _, b := range blocks {
				for _, nodeId := range b.NUMAAffinity {
					if _, ok := assignmentsMemory[nodeId]; !ok {
						assignmentsMemory[nodeId] = map[v1.ResourceName]uint64{}
					}

					if _, ok := assignmentsMemory[nodeId][b.Type]; !ok {
						assignmentsMemory[nodeId][b.Type] = 0
					}
					assignmentsMemory[nodeId][b.Type] += b.Size
				}
			}
		}
	}

	for _, node := range p.machineInfo.Topology {
		nodeState, ok := machineState[node.Id]
		if !ok {
			return fmt.Errorf("[memorymanager] machine state does not have NUMA node %d", node.Id)
		}

		// validated that machine state memory values equals to node values
		if err := p.validateResourceMemory(&node, node.Memory, nodeState.MemoryMap, v1.ResourceMemory); err != nil {
			return err
		}

		// validate that memory assigned to containers equals to reserved one under the machine state
		if err := p.validateResourceReservedMemory(assignmentsMemory[node.Id], nodeState.MemoryMap, v1.ResourceMemory); err != nil {
			return err
		}

		for _, hugepage := range node.HugePages {
			hugepageQuantity := resource.NewQuantity(int64(hugepage.PageSize)*1024, resource.BinarySI)
			resourceName := corehelper.HugePageResourceName(*hugepageQuantity)
			expectedTotal := hugepage.NumPages * hugepage.PageSize * 1024

			// validated that machine state memory values equals to node values
			if err := p.validateResourceMemory(&node, expectedTotal, nodeState.MemoryMap, resourceName); err != nil {
				return err
			}

			// validate that memory assigned to containers equals to reserved one under the machine state
			if err := p.validateResourceReservedMemory(assignmentsMemory[node.Id], nodeState.MemoryMap, resourceName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *singleNUMAPolicy) validateResourceMemory(node *cadvisorapi.Node, expectedTotal uint64, machineMemory map[v1.ResourceName]*state.MemoryTable, resourceName v1.ResourceName) error {
	resourceSize, ok := machineMemory[resourceName]
	if !ok {
		return fmt.Errorf("[memorymanager] machine state does not have %s resource", resourceName)
	}

	if expectedTotal != resourceSize.TotalMemSize {
		return fmt.Errorf("[memorymanager] machine state has different size of the total %s", resourceName)
	}

	if p.systemReserved[node.Id][resourceName] != resourceSize.SystemReserved {
		return fmt.Errorf("[memorymanager] machine state has different size of the system reserved %s", resourceName)
	}
	return nil
}

func (p *singleNUMAPolicy) validateResourceReservedMemory(assignmentsMemory map[v1.ResourceName]uint64, machineMemory map[v1.ResourceName]*state.MemoryTable, resourceName v1.ResourceName) error {
	if assignmentsMemory[resourceName] != machineMemory[resourceName].Reserved {
		return fmt.Errorf("[memorymanager] %s reserved by containers differs from the machine state reserved", resourceName)
	}
	return nil
}

func (p *singleNUMAPolicy) getDefaultNUMAAffinity(s state.State, container *v1.Container, preferred bool) (bitmask.BitMask, error) {
	requestedResources, err := getRequestedResources(container)
	if err != nil {
		return nil, err
	}

	hints := p.calculateHints(s, requestedResources)

	// hints for all memory types should be the same, so we will check hints only for regular memory type
	for _, hint := range hints[string(v1.ResourceMemory)] {
		// get first hint that fit to the preferred parameter
		if hint.Preferred || hint.Preferred == preferred {
			return hint.NUMANodeAffinity, nil
		}
	}

	return nil, fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available")
}
