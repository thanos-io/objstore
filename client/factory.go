// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/azure"
	"github.com/thanos-io/objstore/providers/bos"
	"github.com/thanos-io/objstore/providers/cos"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/gcs"
	"github.com/thanos-io/objstore/providers/obs"
	"github.com/thanos-io/objstore/providers/oci"
	"github.com/thanos-io/objstore/providers/oss"
	"github.com/thanos-io/objstore/providers/s3"
	"github.com/thanos-io/objstore/providers/swift"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type ObjProvider string

const (
	FILESYSTEM ObjProvider = "FILESYSTEM"
	GCS        ObjProvider = "GCS"
	S3         ObjProvider = "S3"
	AZURE      ObjProvider = "AZURE"
	SWIFT      ObjProvider = "SWIFT"
	COS        ObjProvider = "COS"
	ALIYUNOSS  ObjProvider = "ALIYUNOSS"
	BOS        ObjProvider = "BOS"
	OCI        ObjProvider = "OCI"
	OBS        ObjProvider = "OBS"
)

type BucketConfig struct {
	Type   ObjProvider `yaml:"type"`
	Config interface{} `yaml:"config"`
	Prefix string      `yaml:"prefix" default:""`
}

// NewBucket initializes and returns new object storage clients.
// NOTE: confContentYaml can contain secrets.
func NewBucket(logger log.Logger, confContentYaml []byte, component string, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (objstore.Bucket, error) {
	level.Info(logger).Log("msg", "loading bucket configuration")
	bucketConf := &BucketConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, bucketConf); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	config, err := yaml.Marshal(bucketConf.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of bucket configuration")
	}

	var bucket objstore.Bucket
	switch strings.ToUpper(string(bucketConf.Type)) {
	case string(GCS):
		bucket, err = gcs.NewBucket(context.Background(), logger, config, component, wrapRoundtripper)
	case string(S3):
		bucket, err = s3.NewBucket(logger, config, component, wrapRoundtripper)
	case string(AZURE):
		bucket, err = azure.NewBucket(logger, config, component, wrapRoundtripper)
	case string(SWIFT):
		bucket, err = swift.NewContainer(logger, config, wrapRoundtripper)
	case string(COS):
		bucket, err = cos.NewBucket(logger, config, component, wrapRoundtripper)
	case string(ALIYUNOSS):
		bucket, err = oss.NewBucket(logger, config, component, wrapRoundtripper)
	case string(FILESYSTEM):
		bucket, err = filesystem.NewBucketFromConfig(config)
	case string(BOS):
		bucket, err = bos.NewBucket(logger, config, component)
	case string(OCI):
		bucket, err = oci.NewBucket(logger, config, wrapRoundtripper)
	case string(OBS):
		bucket, err = obs.NewBucket(logger, config)
	default:
		return nil, errors.Errorf("bucket with type %s is not supported", bucketConf.Type)
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s client", bucketConf.Type))
	}

	return objstore.NewPrefixedBucket(bucket, bucketConf.Prefix), nil
}
