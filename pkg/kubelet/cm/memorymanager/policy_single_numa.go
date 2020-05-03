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
// TODO: need to re-evaluate what field we really need
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
	if !hint.Preferred || hint.NUMANodeAffinity == nil {
		defaultAffinity, err := getDefaultNUMAAffinity(s, container)
		if err != nil {
			return err
		}
		affinityBits = defaultAffinity.GetBits()
	} else {
		affinityBits = hint.NUMANodeAffinity.GetBits()
	}

	if len(affinityBits) > 1 {
		return fmt.Errorf("[memorymanager] NUMA affinity has more than one NUMA node")
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
		nodeResourceState := machineState[affinityBits[0]][resourceName]
		nodeResourceState.Reserved += uint64(size)
		nodeResourceState.Free -= uint64(size)

		// update memory blocks
		containerBlocks = append(containerBlocks, state.Block{
			NUMAAffinity: affinityBits[0],
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
		nodeResourceState := machineState[b.NUMAAffinity][b.Type]
		nodeResourceState.Free += b.Size
		nodeResourceState.Reserved -= b.Size
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

	hints := map[string][]topologymanager.TopologyHint{}
	containerBlocks := s.GetMemoryBlocks(string(pod.UID), container.Name)
	for resourceName, q := range container.Resources.Requests {
		if resourceName != v1.ResourceMemory && !corehelper.IsHugePageResourceName(resourceName) {
			continue
		}

		requested, succeed := q.AsInt64()
		if !succeed {
			klog.Error("[memorymanager] failed to represent quantity as int64")
		}

		if _, ok := hints[string(resourceName)]; !ok {
			hints[string(resourceName)] = []topologymanager.TopologyHint{}
		}

		// Short circuit to regenerate the same hints if there are already
		// memory allocated for the container. This might happen after a
		// kubelet restart, for example.
		if containerBlocks != nil {
			for _, b := range containerBlocks {
				if b.Type != resourceName {
					continue
				}

				if b.Size != uint64(requested) {
					klog.Errorf("[memorymanager] Memory %s already allocated to (pod %v, container %v) with different number than request: requested: %d, allocated: %d", b.Type, pod.UID, container.Name, requested, b.Size)
					return nil
				}

				containerNUMAAffinity, err := bitmask.NewBitMask(b.NUMAAffinity)
				if err != nil {
					klog.Errorf("[memorymanager] failed to generate NUMA bitmask: %v", err)
					return nil
				}

				klog.Infof("[memorymanager] Regenerating TopologyHints, %s was already allocated to (pod %v, container %v)", resourceName, pod.UID, container.Name)
				hints[string(resourceName)] = append(hints[string(resourceName)], topologymanager.TopologyHint{
					NUMANodeAffinity: containerNUMAAffinity,
					Preferred:        true,
				})
				break
			}
		} else {
			for numaId, memoryState := range s.GetMachineState() {
				if memoryState[resourceName].Free >= uint64(requested) {
					affinity, err := bitmask.NewBitMask(numaId)
					if err != nil {
						klog.Errorf("[memorymanager] failed to generate NUMA bitmask: %v", err)
						return nil
					}
					hints[string(resourceName)] = append(hints[string(resourceName)], topologymanager.TopologyHint{
						NUMANodeAffinity: affinity,
						Preferred:        true,
					})
				}
			}
		}
	}

	return hints
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
			machineState[node.Id] = map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    allocatable,
					Free:           allocatable,
					Reserved:       0,
					SystemReserved: p.systemReserved[node.Id][v1.ResourceMemory],
					TotalMemSize:   node.Memory,
				},
			}

			// fill memory table with huge pages values
			for _, hugepage := range node.HugePages {
				hugepageQuantity := resource.NewQuantity(int64(hugepage.PageSize)*1024, resource.BinarySI)
				resourceName := corehelper.HugePageResourceName(*hugepageQuantity)
				// TODO: once it will be possible to reserve huge pages for system usage, we should update values
				machineState[node.Id][resourceName] = &state.MemoryTable{
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
				if _, ok := assignmentsMemory[b.NUMAAffinity]; !ok {
					assignmentsMemory[b.NUMAAffinity] = map[v1.ResourceName]uint64{}
				}

				if _, ok := assignmentsMemory[b.NUMAAffinity][b.Type]; !ok {
					assignmentsMemory[b.NUMAAffinity][b.Type] = 0
				}
				assignmentsMemory[b.NUMAAffinity][b.Type] += b.Size
			}
		}
	}

	for _, node := range p.machineInfo.Topology {
		machineMemory, ok := machineState[node.Id]
		if !ok {
			return fmt.Errorf("[memorymanager] machine state does not have NUMA node %d", node.Id)
		}

		// validated that machine state memory values equals to node values
		if err := p.validateResourceMemory(&node, node.Memory, machineMemory, v1.ResourceMemory); err != nil {
			return err
		}

		// validate that memory assigned to containers equals to reserved one under the machine state
		if err := p.validateResourceReservedMemory(assignmentsMemory[node.Id], machineMemory, v1.ResourceMemory); err != nil {
			return err
		}

		for _, hugepage := range node.HugePages {
			hugepageQuantity := resource.NewQuantity(int64(hugepage.PageSize)*1024, resource.BinarySI)
			resourceName := corehelper.HugePageResourceName(*hugepageQuantity)
			expectedTotal := hugepage.NumPages * hugepage.PageSize * 1024

			// validated that machine state memory values equals to node values
			if err := p.validateResourceMemory(&node, expectedTotal, machineMemory, resourceName); err != nil {
				return err
			}

			// validate that memory assigned to containers equals to reserved one under the machine state
			if err := p.validateResourceReservedMemory(assignmentsMemory[node.Id], machineMemory, resourceName); err != nil {
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

func getDefaultNUMAAffinity(s state.State, container *v1.Container) (bitmask.BitMask, error) {
	machineState := s.GetMachineState()

	var nodeID = -1
	for id, memoryState := range machineState {
		for resourceName, q := range container.Resources.Requests {
			if resourceName != v1.ResourceMemory && !corehelper.IsHugePageResourceName(resourceName) {
				continue
			}

			size, succeed := q.AsInt64()
			if !succeed {
				return nil, fmt.Errorf("[memorymanager] failed to represent quantity as int64")
			}

			resourceState := memoryState[resourceName]
			if resourceState.Free < uint64(size) {
				nodeID = -1
				break
			}
			nodeID = id
		}

		if nodeID != -1 {
			defaultNUMAAffinity, err := bitmask.NewBitMask(nodeID)
			if err != nil {
				return nil, err
			}
			return defaultNUMAAffinity, nil
		}
	}
	return nil, fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available")
}
