package oss

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/thanos-io/objstore/errutil"
)

func Test_validate(t *testing.T) {
	type args struct {
		name   string
		config Config
		error  bool
	}
	tests := []args{
		{
			name: "empty endpoint",
			config: Config{
				Endpoint:        "",
				Bucket:          "test",
				AccessKeyID:     "foo",
				AccessKeySecret: "bar",
			},
			error: true,
		},
		{
			name: "empty bucket",
			config: Config{
				Endpoint:        "http://test.com/",
				Bucket:          "",
				AccessKeyID:     "foo",
				AccessKeySecret: "bar",
			},
			error: true,
		},
		{
			name: "empty access key id",
			config: Config{
				Endpoint:        "http://test.com/",
				Bucket:          "test",
				AccessKeyID:     "",
				AccessKeySecret: "bar",
			},
			error: true,
		},
		{
			name: "empty access key secret",
			config: Config{
				Endpoint:        "http://test.com/",
				Bucket:          "test",
				AccessKeyID:     "foo",
				AccessKeySecret: "",
			},
			error: true,
		},
		{
			name: "no error 1",
			config: Config{
				Endpoint:        "http://test.com/",
				Bucket:          "test",
				AccessKeyID:     "foo",
				AccessKeySecret: "bar",
			},
			error: false,
		},
		{
			name: "no error 2",
			config: Config{
				Endpoint: "http://test.com/",
				Bucket:   "test",
				SdkAuth:  true,
			},
			error: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(tt.config)
			if tt.error {
				testutil.NotOk(t, err, "Expected error, got none")
			} else {
				testutil.Ok(t, err, "Expected no error, got: %v", err)
			}
		})
	}
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	config := Config{
		Endpoint:        "http://test.com/",
		AccessKeyID:     "123",
		AccessKeySecret: "123",
		Bucket:          "test",
	}

	bkt, err := NewBucketWithConfig(log.NewNopLogger(), config, "test", errutil.WrapWithErrRoundtripper)
	// We expect an error from the RoundTripper
	testutil.Ok(t, err)
	_, err = bkt.Get(context.Background(), "test")
	testutil.NotOk(t, err)
	testutil.Assert(t, errutil.IsMockedError(err), "Expected RoundTripper error, got: %v", err)
}
