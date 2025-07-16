// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientutil

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

func TrimExtraFields(confContentYaml []byte) ([]byte, error) {
	var raw map[string]interface{}
	if err := yaml.Unmarshal(confContentYaml, &raw); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal YAML")
	}
	allowed := map[string]bool{
		"type":   true,
		"config": true,
		"prefix": true,
	}
	filtered := make(map[string]interface{})
	for key, value := range raw {
		if allowed[key] {
			filtered[key] = value
		}
	}
	return yaml.Marshal(filtered)
}
