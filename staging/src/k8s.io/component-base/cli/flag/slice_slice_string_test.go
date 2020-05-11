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
	"reflect"
	"testing"
)

func TestStringSliceSliceString(t *testing.T) {
	var nilSliceSlice [][]string
	testCases := []struct {
		desc   string
		m      *SliceSliceString
		expect string
	}{
		{"nil", NewSliceSliceString(&nilSliceSlice), ""},
		{"empty", NewSliceSliceString(&[][]string{}), ""},
		{"one value", NewSliceSliceString(&[][]string{{"value1"}}), "[value1]"},
		{"two values", NewSliceSliceString(&[][]string{{"value1", "value2"}}), "[value1,value2]"},
		{"two slices", NewSliceSliceString(&[][]string{{"value1", "value2"}, {"value3", "value4"}}), "[value1,value2],[value3,value4]"},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			str := tc.m.String()
			if tc.expect != str {
				t.Fatalf("expect %q but got %q", tc.expect, str)
			}
		})
	}
}

func TestSetSliceSliceString(t *testing.T) {
	var nilSliceSlice [][]string
	testCases := []struct {
		desc   string
		vals   []string
		start  *SliceSliceString
		expect *SliceSliceString
		err    string
	}{
		// we initialize the slice with a default key that should be cleared by Set
		{"clears defaults", []string{""},
			NewSliceSliceString(&[][]string{{"default"}}),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{},
			}, ""},
		// make sure we still allocate for "initialized" multi slice where multi slice was initially set to a nil slice
		{"allocates slice if currently nil", []string{""},
			&SliceSliceString{initialized: true, Slice: &nilSliceSlice},
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{},
			}, ""},
		// for most cases, we just reuse nilSlice, which should be allocated by Set, and is reset before each test case
		{"empty", []string{""},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{},
			}, ""},
		{"empty slice", []string{"[]"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{},
			}, ""},
		{"missing square brackets", []string{"value1, value2"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{{"value1", "value2"}},
			}, ""},
		{"one value", []string{"[value1]]"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{{"value1"}},
			}, ""},
		{"two values", []string{"[value1,value2]"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{{"value1", "value2"}},
			}, ""},
		{"two duplicated values", []string{"[value1,value1]"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{{"value1", "value1"}},
			}, ""},
		{"two values with spaces", []string{"[  value1  ,  value2  ]"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{{"value1", "value2"}},
			}, ""},
		{"two values, multiple Set invocations", []string{"[value1, value2]", "[value3, value4]"},
			NewSliceSliceString(&nilSliceSlice),
			&SliceSliceString{
				initialized: true,
				Slice:       &[][]string{{"value1", "value2"}, {"value3", "value4"}},
			}, ""},
		{"no target", []string{""},
			NewSliceSliceString(nil),
			nil,
			"no target (nil pointer to [][]string)"},
	}
	for _, tc := range testCases {
		nilSliceSlice = nil
		t.Run(tc.desc, func(t *testing.T) {
			var err error
			for _, val := range tc.vals {
				err = tc.start.Set(val)
				if err != nil {
					break
				}
			}
			if tc.err != "" {
				if err == nil || err.Error() != tc.err {
					t.Fatalf("expect error %s but got %v", tc.err, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(tc.expect, tc.start) {
				t.Fatalf("expect %#v but got %#v", tc.expect, tc.start)
			}
		})
	}
}

func TestEmptySliceSliceString(t *testing.T) {
	var nilSliceSlice [][]string
	testCases := []struct {
		desc   string
		m      *SliceSliceString
		expect bool
	}{
		{"nil", NewSliceSliceString(&nilSliceSlice), true},
		{"empty", NewSliceSliceString(&[][]string{}), true},
		{"populated", NewSliceSliceString(&[][]string{{"value1", "value2"}}), false},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret := tc.m.Empty()
			if ret != tc.expect {
				t.Fatalf("expect %t but got %t", tc.expect, ret)
			}
		})
	}
}
