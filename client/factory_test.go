// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/go-kit/log"
	"go.opentelemetry.io/otel/trace"

	"github.com/thanos-io/objstore/tracing/opentelemetry"
	"github.com/thanos-io/objstore/tracing/opentracing"
)

func ExampleBucket() {
	// Read the configuration file.
	confContentYaml, err := ioutil.ReadFile("testconf/filesystem.conf.yml")
	if err != nil {
		panic(err)
	}

	// Create a new bucket.
	bucket, err := NewBucket(log.NewNopLogger(), confContentYaml, "example")
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

func ExampleTracingBucketUsingOpenTracing() { //nolint:govet
	// Read the configuration file.
	confContentYaml, err := ioutil.ReadFile("testconf/filesystem.conf.yml")
	if err != nil {
		panic(err)
	}

	// Create a new bucket.
	bucket, err := NewBucket(log.NewNopLogger(), confContentYaml, "example")
	if err != nil {
		panic(err)
	}

	// Wrap it with tracing.
	bucket = opentracing.WrapWithTraces(bucket)

	// Test it.
	exists, err := bucket.Exists(context.Background(), "example")
	if err != nil {
		panic(err)
	}
	fmt.Println(exists)
	// Output:
	// false
}

func ExampleTracingBucketUsingOpenTelemetry() { //nolint:govet
	// Read the configuration file.
	confContentYaml, err := ioutil.ReadFile("testconf/filesystem.conf.yml")
	if err != nil {
		panic(err)
	}

	// Create a new bucket.
	bucket, err := NewBucket(log.NewNopLogger(), confContentYaml, "example")
	if err != nil {
		panic(err)
	}

	// Wrap it with tracing.
	bucket = opentelemetry.WrapWithTraces(bucket, trace.NewNoopTracerProvider().Tracer("bucket"))

	// Test it.
	exists, err := bucket.Exists(context.Background(), "example")
	if err != nil {
		panic(err)
	}
	fmt.Println(exists)
	// Output:
	// false
}
