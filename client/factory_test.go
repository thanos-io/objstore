// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"fmt"
	"os"

	"github.com/go-kit/log"
)

func ExampleBucket() {
	// Read the configuration file.
	confContentYaml, err := os.ReadFile("testconf/filesystem.conf.yml")
	if err != nil {
		panic(err)
	}

	// Create a new bucket.
	bucket, err := NewBucket(log.NewNopLogger(), confContentYaml, "example", nil)
	if err != nil {
		panic(err)
	}

	// Test it.
	exists, err := bucket.Exists(context.Background(), "example")
	if err != nil {
		panic(err)
	}
	fmt.Println(exists)
	// Output:
	// false
}
