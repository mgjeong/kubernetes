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
	// machineInfo contains machine memory related information
	machineInfo *cadvisorapi.MachineInfo
	// reserved contains memory that reserved for kube
	systemReserved systemReservedMemory
	// topology manager reference to get container Topology affinity
	affinity topologymanager.Store
}

var _ Policy = &singleNUMAPolicy{}

// NewPolicySingleNUMA returns new single NUMA policy instance
func NewPolicySingleNUMA(machineInfo *cadvisorapi.MachineInfo, reserved systemReservedMemory, affinity topologymanager.Store) (Policy, error) {
	var totalSystemReserved uint64
	for _, node := range reserved {
		if _, ok := node[v1.ResourceMemory]; !ok {
			continue
		}
		totalSystemReserved += node[v1.ResourceMemory]
	}

	// check if we have some reserved memory for the system
	if totalSystemReserved <= 0 {
		return nil, fmt.Errorf("[memorymanager] you should specify the memory reserved for the system")
	}

	return &singleNUMAPolicy{
		machineInfo:    machineInfo,
		systemReserved: reserved,
		affinity:       affinity,
	}, nil
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

	requestedResources, err := getRequestedResources(container)
	if err != nil {
		return err
	}

	bestHint := &hint
	// topology manager returned the hint with NUMA affinity nil
	// we should use the default NUMA affinity calculated the same way as for the topology manager
	if hint.NUMANodeAffinity == nil {
		defaultHint, err := p.getDefaultHint(s, requestedResources)
		if err != nil {
			return err
		}

		if !defaultHint.Preferred && bestHint.Preferred {
			return fmt.Errorf("[memorymanager] failed to find the default preferred hint")
		}
		bestHint = defaultHint
	}

	machineState := s.GetMachineState()

	// topology manager returns the hint that does not satisfy completely the container request
	// we should extend this hint to the one who will satisfy the request and include the current hint
	if !isAffinitySatisfyRequest(machineState, bestHint.NUMANodeAffinity, requestedResources) {
		extendedHint, err := p.extendTopologyManagerHint(s, requestedResources, bestHint.NUMANodeAffinity)
		if err != nil {
			return err
		}

		if !extendedHint.Preferred && bestHint.Preferred {
			return fmt.Errorf("[memorymanager] failed to find the extended preferred hint")
		}
		bestHint = extendedHint
	}

	var containerBlocks []state.Block
	maskBits := bestHint.NUMANodeAffinity.GetBits()
	for resourceName, requestedSize := range requestedResources {
		// update memory blocks
		containerBlocks = append(containerBlocks, state.Block{
			NUMAAffinity: maskBits,
			Size:         requestedSize,
			Type:         resourceName,
		})

		// Update nodes memory state
		for _, nodeId := range maskBits {
			machineState[nodeId].NumberOfAssignments++
			machineState[nodeId].Nodes = maskBits

			// we need to continue to update all affinity mask nodes
			if requestedSize == 0 {
				continue
			}

			// update the node memory state
			nodeResourceMemoryState := machineState[nodeId].MemoryMap[resourceName]
			if nodeResourceMemoryState.Free <= 0 {
				continue
			}

			// the node has enough memory to satisfy the request
			if nodeResourceMemoryState.Free >= requestedSize {
				nodeResourceMemoryState.Reserved += requestedSize
				nodeResourceMemoryState.Free -= requestedSize
				requestedSize = 0
				continue
			}

			// the node does not have enough memory, use the node remaining memory and move to the next node
			requestedSize -= nodeResourceMemoryState.Free
			nodeResourceMemoryState.Reserved += nodeResourceMemoryState.Free
			nodeResourceMemoryState.Free = 0
		}
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
		releasedSize := b.Size
		for _, nodeId := range b.NUMAAffinity {
			machineState[nodeId].NumberOfAssignments--

			// once we do not have any memory allocations on this node, clear node groups
			if machineState[nodeId].NumberOfAssignments == 0 {
				machineState[nodeId].Nodes = []int{nodeId}
			}

			// we still need to pass over all NUMA node under the affinity mask to update them
			if releasedSize == 0 {
				continue
			}

			nodeResourceMemoryState := machineState[nodeId].MemoryMap[b.Type]

			// if the node does not have reserved memory to free, continue to the next node
			if nodeResourceMemoryState.Reserved == 0 {
				continue
			}

			// the reserved memory smaller than the amount of the memory that should be released
			// release as much as possible and move to the next node
			if nodeResourceMemoryState.Reserved < releasedSize {
				nodeResourceMemoryState.Free += nodeResourceMemoryState.Reserved
				nodeResourceMemoryState.Reserved = 0
				releasedSize -= nodeResourceMemoryState.Reserved
				continue
			}

			// the reserved memory big enough to satisfy the released memory
			nodeResourceMemoryState.Free += releasedSize
			nodeResourceMemoryState.Reserved -= releasedSize
			releasedSize = 0
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
			klog.Errorf("[memorymanager] The number of requested resources by the container %s differs from the number of memory blocks", container.Name)
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
	machineState := s.GetMachineState()
	var numaNodes []int
	for n := range machineState {
		numaNodes = append(numaNodes, n)
	}
	sort.Ints(numaNodes)

	// Initialize minAffinitySize to include all NUMA Nodes.
	minAffinitySize := len(numaNodes)

	hints := map[string][]topologymanager.TopologyHint{}
	bitmask.IterateBitMasks(numaNodes, func(mask bitmask.BitMask) {
		maskBits := mask.GetBits()
		singleNUMAHint := len(maskBits) == 1

		// the node already in group with another node, it can not be used for the single NUMA node allocation
		if singleNUMAHint && len(machineState[maskBits[0]].Nodes) > 1 {
			return
		}

		totalFreeSize := map[v1.ResourceName]uint64{}
		totalAllocatableSize := map[v1.ResourceName]uint64{}
		// calculate total free memory for the node mask
		for _, nodeId := range maskBits {
			// the node already used for the memory allocation
			if !singleNUMAHint && machineState[nodeId].NumberOfAssignments > 0 {
				// the node used for the single NUMA memory allocation, it can be used for the multi NUMA node allocation
				if len(machineState[nodeId].Nodes) == 1 {
					return
				}

				// the node already used with different group of nodes, it can not be use with in the current hint
				if !areGroupsEqual(machineState[nodeId].Nodes, maskBits) {
					return
				}
			}

			for resourceName := range requestedResources {
				if _, ok := totalFreeSize[resourceName]; !ok {
					totalFreeSize[resourceName] = 0
				}
				totalFreeSize[resourceName] += machineState[nodeId].MemoryMap[resourceName].Free

				if _, ok := totalAllocatableSize[resourceName]; !ok {
					totalAllocatableSize[resourceName] = 0
				}
				totalAllocatableSize[resourceName] += machineState[nodeId].MemoryMap[resourceName].Allocatable
			}
		}

		// verify that for all memory types the node mask has enough allocatable resources
		for resourceName, requestedSize := range requestedResources {
			if totalAllocatableSize[resourceName] < requestedSize {
				return
			}
		}

		// set the minimum amount of NUMA nodes that can satisfy the container resources requests
		if mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// verify that for all memory types the node mask has enough free resources
		for resourceName, requestedSize := range requestedResources {
			if totalFreeSize[resourceName] < requestedSize {
				return
			}
		}

		// add the node mask as topology hint for all memory types
		for resourceName := range requestedResources {
			if _, ok := hints[string(resourceName)]; !ok {
				hints[string(resourceName)] = []topologymanager.TopologyHint{}
			}
			hints[string(resourceName)] = append(hints[string(resourceName)], topologymanager.TopologyHint{
				NUMANodeAffinity: mask,
				Preferred:        false,
			})
		}
	})

	// update hints preferred according to multiNUMAGroups, in case when it wasn't provided, the default
	// behaviour to prefer the minimal amount of NUMA nodes will be used
	for resourceName := range requestedResources {
		for i, hint := range hints[string(resourceName)] {
			hints[string(resourceName)][i].Preferred = p.isHintPreferred(hint.NUMANodeAffinity.GetBits(), minAffinitySize)
		}
	}

	return hints
}

func (p *singleNUMAPolicy) isHintPreferred(maskBits []int, minAffinitySize int) bool {
	return len(maskBits) == minAffinitySize
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

func (p *singleNUMAPolicy) validateState(s state.State) error {
	machineState := s.GetMachineState()
	memoryAssignments := s.GetMemoryAssignments()

	if len(machineState) == 0 {
		// Machine state cannot be empty when assignments exist
		if len(memoryAssignments) != 0 {
			return fmt.Errorf("[memorymanager] machine state can not be empty when it has memory assignments")
		}

		defaultMachineState := p.getDefaultMachineState()
		s.SetMachineState(defaultMachineState)

		return nil
	}

	// calculate all memory assigned to containers
	expectedMachineState := p.getDefaultMachineState()
	for pod, container := range memoryAssignments {
		for containerName, blocks := range container {
			for _, b := range blocks {
				requestedSize := b.Size
				for _, nodeId := range b.NUMAAffinity {
					nodeState, ok := expectedMachineState[nodeId]
					if !ok {
						return fmt.Errorf("[memorymanager] (pod: %s, container: %s) the memory assignment uses the NUMA that does not exist", pod, containerName)
					}

					nodeState.NumberOfAssignments++
					nodeState.Nodes = b.NUMAAffinity

					memoryState, ok := nodeState.MemoryMap[b.Type]
					if !ok {
						return fmt.Errorf("[memorymanager] (pod: %s, container: %s) the memory assignment uses memory resource that does not exist", pod, containerName)
					}

					if requestedSize == 0 {
						continue
					}

					// this node does not have enough memory continue to the next one
					if memoryState.Free <= 0 {
						continue
					}

					// the node has enough memory to satisfy the request
					if memoryState.Free >= requestedSize {
						memoryState.Reserved += requestedSize
						memoryState.Free -= requestedSize
						requestedSize = 0
						continue
					}

					// the node does not have enough memory, use the node remaining memory and move to the next node
					requestedSize -= memoryState.Free
					memoryState.Reserved += memoryState.Free
					memoryState.Free = 0
				}
			}
		}
	}

	// State has already been initialized from file (is not empty)
	// Validate that total size, system reserved and reserved memory not changed, it can happen, when:
	// - adding or removing physical memory bank from the node
	// - change of kubelet system-reserved, kube-reserved or pre-reserved-memory-zone parameters
	if !areMachineStatesEqual(machineState, expectedMachineState) {
		return fmt.Errorf("[memorymanager] the expected machine state is different from the real one")
	}

	return nil
}

func areMachineStatesEqual(ms1, ms2 state.NodeMap) bool {
	if len(ms1) != len(ms2) {
		klog.Errorf("[memorymanager] node states are different len(ms1) != len(ms2): %d != %d", len(ms1), len(ms2))
		return false
	}

	for nodeId, nodeState1 := range ms1 {
		nodeState2, ok := ms2[nodeId]
		if !ok {
			klog.Errorf("[memorymanager] node state does not have node ID %d", nodeId)
			return false
		}

		if nodeState1.NumberOfAssignments != nodeState2.NumberOfAssignments {
			klog.Errorf("[memorymanager] node states number of assignments are different: %d != %d", nodeState1.NumberOfAssignments, nodeState2.NumberOfAssignments)
			return false
		}

		if !areGroupsEqual(nodeState1.Nodes, nodeState2.Nodes) {
			klog.Errorf("[memorymanager] node states groups are different: %v != %v", nodeState1.Nodes, nodeState2.Nodes)
			return false
		}

		if len(nodeState1.MemoryMap) != len(nodeState2.MemoryMap) {
			klog.Errorf("[memorymanager] node states memory map have different length: %d != %d", len(nodeState1.MemoryMap), len(nodeState2.MemoryMap))
			return false
		}

		for resourceName, memoryState1 := range nodeState1.MemoryMap {
			memoryState2, ok := nodeState2.MemoryMap[resourceName]
			if !ok {
				klog.Errorf("[memorymanager] memory state does not have resource %s", resourceName)
				return false
			}

			if !reflect.DeepEqual(*memoryState1, *memoryState2) {
				klog.Errorf("[memorymanager] memory states for the NUMA node %d and the resource %s are different: %+v != %+v", nodeId, resourceName, *memoryState1, *memoryState2)
				return false
			}
		}
	}
	return true
}

func (p *singleNUMAPolicy) getDefaultMachineState() state.NodeMap {
	defaultMachineState := state.NodeMap{}
	nodeHugepages := map[int]uint64{}
	for _, node := range p.machineInfo.Topology {
		defaultMachineState[node.Id] = &state.NodeState{
			NumberOfAssignments: 0,
			MemoryMap:           map[v1.ResourceName]*state.MemoryTable{},
			Nodes:               []int{node.Id},
		}

		// fill memory table with huge pages values
		for _, hugepage := range node.HugePages {
			hugepageQuantity := resource.NewQuantity(int64(hugepage.PageSize)*1024, resource.BinarySI)
			resourceName := corehelper.HugePageResourceName(*hugepageQuantity)
			systemReserved := p.getResourceSystemReserved(node.Id, resourceName)
			totalHugepagesSize := hugepage.NumPages * hugepage.PageSize * 1024
			allocatable := totalHugepagesSize - systemReserved
			defaultMachineState[node.Id].MemoryMap[resourceName] = &state.MemoryTable{
				Allocatable:    allocatable,
				Free:           allocatable,
				Reserved:       0,
				SystemReserved: systemReserved,
				TotalMemSize:   totalHugepagesSize,
			}
			if _, ok := nodeHugepages[node.Id]; !ok {
				nodeHugepages[node.Id] = 0
			}
			nodeHugepages[node.Id] += totalHugepagesSize
		}

		// fill memory table with regular memory values
		systemReserved := p.getResourceSystemReserved(node.Id, v1.ResourceMemory)

		allocatable := node.Memory - systemReserved
		// remove memory allocated by hugepages
		if allocatedByHugepages, ok := nodeHugepages[node.Id]; ok {
			allocatable -= allocatedByHugepages
		}
		defaultMachineState[node.Id].MemoryMap[v1.ResourceMemory] = &state.MemoryTable{
			Allocatable:    allocatable,
			Free:           allocatable,
			Reserved:       0,
			SystemReserved: systemReserved,
			TotalMemSize:   node.Memory,
		}
	}
	return defaultMachineState
}

func (p *singleNUMAPolicy) getResourceSystemReserved(nodeId int, resourceName v1.ResourceName) uint64 {
	var systemReserved uint64
	if nodeSystemReserved, ok := p.systemReserved[nodeId]; ok {
		if nodeMemorySystemReserved, ok := nodeSystemReserved[resourceName]; ok {
			systemReserved = nodeMemorySystemReserved
		}
	}
	return systemReserved
}

func (p *singleNUMAPolicy) getDefaultHint(s state.State, requestedResources map[v1.ResourceName]uint64) (*topologymanager.TopologyHint, error) {
	hints := p.calculateHints(s, requestedResources)
	if len(hints) < 1 {
		return nil, fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available")
	}

	// hints for all memory types should be the same, so we will check hints only for regular memory type
	return findBestHint(hints[string(v1.ResourceMemory)]), nil
}

func isAffinitySatisfyRequest(machineState state.NodeMap, mask bitmask.BitMask, requestedResources map[v1.ResourceName]uint64) bool {
	totalFreeSize := map[v1.ResourceName]uint64{}
	for _, nodeId := range mask.GetBits() {
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
			return false
		}
	}

	return true
}

// extendTopologyManagerHint extends the topology manager hint, in case when it does not satisfy to the container request
// the topology manager uses bitwise AND to merge all topology hints into the best one, so in case of the restricted policy,
// it possible that we will get the subset of hint that we provided to the topology manager, in this case we want to extend
// it to the original one
func (p *singleNUMAPolicy) extendTopologyManagerHint(s state.State, requestedResources map[v1.ResourceName]uint64, mask bitmask.BitMask) (*topologymanager.TopologyHint, error) {
	hints := p.calculateHints(s, requestedResources)

	var filteredHints []topologymanager.TopologyHint
	// hints for all memory types should be the same, so we will check hints only for regular memory type
	for _, hint := range hints[string(v1.ResourceMemory)] {
		affinityBits := hint.NUMANodeAffinity.GetBits()
		// filter all hints that does not include currentHint
		if isHintInGroup(mask.GetBits(), affinityBits) {
			filteredHints = append(filteredHints, hint)
		}
	}

	if len(filteredHints) < 1 {
		return nil, fmt.Errorf("[memorymanager] failed to find NUMA nodes to extend the current topology hint")
	}

	// try to find the preferred hint with the minimal number of NUMA nodes, relevant for the restricted policy
	return findBestHint(filteredHints), nil
}

func isHintInGroup(hint []int, group []int) bool {
	sort.Ints(hint)
	sort.Ints(group)

	hintIndex := 0
	for i := range group {
		if hintIndex == len(hint) {
			return true
		}

		if group[i] != hint[hintIndex] {
			continue
		}
		hintIndex++
	}
	return false
}

func findBestHint(hints []topologymanager.TopologyHint) *topologymanager.TopologyHint {
	// try to find the preferred hint with the minimal number of NUMA nodes, relevant for the restricted policy
	bestHint := topologymanager.TopologyHint{}
	for _, hint := range hints {
		if bestHint.NUMANodeAffinity == nil {
			bestHint = hint
			continue
		}

		// preferred of the current hint is true, when the extendedHint preferred is false
		if hint.Preferred && !bestHint.Preferred {
			bestHint = hint
			continue
		}

		// both hints has the same preferred value, but the current hint has less NUMA nodes than the extended one
		if hint.Preferred == bestHint.Preferred && hint.NUMANodeAffinity.IsNarrowerThan(bestHint.NUMANodeAffinity) {
			bestHint = hint
		}
	}
	return &bestHint
}
