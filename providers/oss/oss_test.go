package oss

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore/errutil"
)

func TestNewBucketWithErrorRoundTripper(t *testing.T) {

	var config Config
	config.Endpoint = "http://test.com/"
	config.AccessKeyID = "123"
	config.AccessKeySecret = "123"
	config.Bucket = "test"
	config.AccessKeySecret = "123"

	rt := &errutil.ErrorRoundTripper{Err: errors.New("RoundTripper error")}

	bkt, err := NewBucketWithConfig(log.NewNopLogger(), config, "test", rt)
	// We expect an error from the RoundTripper
	testutil.Ok(t, err)
	_, err = bkt.Get(context.Background(), "test")
	testutil.NotOk(t, err)
	testutil.Assert(t, errors.Is(err, rt.Err), "Expected RoundTripper error, got: %v", err)
}
