// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientutil

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v2"
)

func TestTrimExtraFields(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  map[string]interface{}
		expectErr bool
	}{
		{
			name: "YAML with extra field",
			input: `
type: "s3"
config:
  key1: "value1"
  key2: "value2"
prefix: "/path/to/bucket"
hedging_config:
  enabled: false
  up_to: 3
  quantile: 0.9
`,
			expected: map[string]interface{}{
				"type":   "s3",
				"config": map[interface{}]interface{}{"key1": "value1", "key2": "value2"},
				"prefix": "/path/to/bucket",
			},
			expectErr: false,
		},
		{
			name: "YAML without extra field",
			input: `
type: "s3"
config:
  key: "value"
prefix: "/path/to/bucket"
`,
			expected: map[string]interface{}{
				"type":   "s3",
				"config": map[interface{}]interface{}{"key": "value"},
				"prefix": "/path/to/bucket",
			},
			expectErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output, err := TrimExtraFields([]byte(tc.input))
			if (err != nil) != tc.expectErr {
				t.Fatalf("TrimExtraFields() error = %v, expectErr %v", err, tc.expectErr)
			}
			if err != nil {
				return
			}

			var got map[string]interface{}
			if err := yaml.Unmarshal(output, &got); err != nil {
				t.Fatalf("failed to unmarshal output YAML: %v", err)
			}

			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("TrimExtraFields() got = %v, expected %v", got, tc.expected)
			}
		})
	}
}
