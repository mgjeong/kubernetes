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

package flag

import (
	"fmt"
	"sort"
	"strings"
)

// SliceSliceString can be set from the command line with the format `--flag "[0,1]"`.
// Multiple comma-separated slices in a single invocation are supported. For example: `--flag [0,1],[2,3]`.
// is set to false. For example: `--flag "a=foo,b=bar"`.
type SliceSliceString struct {
	Slice       *[][]string
	initialized bool // set to true after the first Set call
}

// NewSliceSliceString takes a pointer to a [][]int and returns the
// SliceSliceString flag parsing shim for that slice
func NewSliceSliceString(s *[][]string) *SliceSliceString {
	return &SliceSliceString{Slice: s}
}

// String implements github.com/spf13/pflag.Value
func (s *SliceSliceString) String() string {
	if s == nil || s.Slice == nil {
		return ""
	}

	var slices []string
	for _, slice := range *s.Slice {
		slices = append(slices, fmt.Sprintf("[%s]", strings.Join(slice, ",")))
	}
	sort.Strings(slices)
	return strings.Join(slices, ",")
}

// Set implements github.com/spf13/pflag.Value
func (s *SliceSliceString) Set(value string) error {
	if s.Slice == nil {
		return fmt.Errorf("no target (nil pointer to [][]string)")
	}
	if !s.initialized || *s.Slice == nil {
		// clear default values, or allocate if no existing map
		*s.Slice = make([][]string, 0)
		s.initialized = true
	}

	value = strings.TrimSpace(value)

	for _, split := range strings.Split(value, ",[") {
		split = strings.TrimLeft(split, "[")
		split = strings.TrimRight(split, "]")

		if len(split) == 0 {
			continue
		}

		var values []string
		for _, v := range strings.Split(split, ",") {
			v = strings.TrimSpace(v)
			values = append(values, v)
		}

		*s.Slice = append(*s.Slice, values)
	}
	return nil

}

// Type implements github.com/spf13/pflag.Value
func (s *SliceSliceString) Type() string {
	return "sliceSliceString"
}

// Empty implements OmitEmpty
func (s *SliceSliceString) Empty() bool {
	return len(*s.Slice) == 0
}
