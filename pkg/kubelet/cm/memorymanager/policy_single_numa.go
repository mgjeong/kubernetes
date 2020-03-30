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
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
)

const policyTypeSingleNUMA policyType = "single-numa"

// TODO: need to implement all methods

// SingleNUMAPolicy is implementation of the policy interface for the single NUMA policy
// TODO: need to re-evaluate what field we really need
type singleNUMAPolicy struct {
	// topology manager reference to get container Topology affinity
	affinity topologymanager.Store
}

var _ Policy = &singleNUMAPolicy{}

// NewPolicySingleNUMA returns new single NUMA policy instance
func NewPolicySingleNUMA() Policy {
	return &singleNUMAPolicy{}
}

func (p *singleNUMAPolicy) Name() string {
	return string(policyTypeSingleNUMA)
}

func (p *singleNUMAPolicy) Start(s state.State) error {
	return nil
}

// Allocate call is idempotent
func (p *singleNUMAPolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	return nil
}

// RemoveContainer call is idempotent
func (p *singleNUMAPolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	return nil
}

// GetTopologyHints implements the topologymanager.HintProvider Interface
// and is consulted to achieve NUMA aware resource alignment among this
// and other resource controllers.
func (p *singleNUMAPolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	return nil
}
