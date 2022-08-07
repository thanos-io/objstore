// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storj

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"gopkg.in/yaml.v2"
	"storj.io/uplink"
)

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {

	ctx := context.Background()
	bktName := objstore.CreateTemporaryTestBucketName(t)
	access := os.Getenv("STORJ_ACCESS")

	storjConfig, err := yaml.Marshal(Config{AccessGrant: access, Bucket: bktName})
	if err != nil {
		return nil, nil, err
	}

	bkt, err := NewBucket(log.NewNopLogger(), storjConfig, "testing")
	if err != nil {
		t.Errorf("failed to create temporary Storj DCS bucket '%s' for testing", bktName)
		return nil, nil, err
	}

	t.Logf("created temporary Storj DCS bucket '%s' for testing", bkt.Name())
	return bkt, func() {
		objstore.EmptyBucket(t, ctx, bkt)
		if _, err := bkt.deleteBucket(ctx, bkt.Name()); err != nil {
			t.Logf("failed to delete temporary Storj DCS bucket %s for testing: %s", bkt.Name(), err)
		}
		t.Logf("deleted temporary Storj DCS bucket '%s' for testing", bkt.Name())
	}, nil
}

// deleteBucket deletes an empty bucket and otherwise returns an error.
// Intentionally not using DeleteBucketWithObjects to ensure not deleting production data.
func (b *Bucket) deleteBucket(ctx context.Context, name string) (*uplink.Bucket, error) {
	return b.project.DeleteBucket(ctx, name)
}
