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

func TestTestBucketConfigFromEnv(t *testing.T) {
	for _, tt := range []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr bool
	}{
		{
			name: "static keys",
			env: map[string]string{
				"ALIYUNOSS_ENDPOINT":          "http://test.com/",
				"ALIYUNOSS_ACCESS_KEY_ID":     "foo",
				"ALIYUNOSS_ACCESS_KEY_SECRET": "bar",
			},
			want: Config{
				Endpoint:        "http://test.com/",
				AccessKeyID:     "foo",
				AccessKeySecret: "bar",
			},
		},
		{
			name: "empty endpoint",
			env: map[string]string{
				"ALIYUNOSS_ACCESS_KEY_ID":     "foo",
				"ALIYUNOSS_ACCESS_KEY_SECRET": "bar",
			},
			wantErr: true,
		},
		{
			name: "missing static keys without sdk_auth",
			env: map[string]string{
				"ALIYUNOSS_ENDPOINT": "http://test.com/",
			},
			wantErr: true,
		},
		{
			name: "missing static keys with sdk_auth",
			env: map[string]string{
				"ALIYUNOSS_ENDPOINT": "http://test.com/",
				"ALIYUNOSS_SDK_AUTH": "true",
			},
			want: Config{
				Endpoint: "http://test.com/",
				SdkAuth:  true,
			},
		},
		{
			name: "invalid sdk_auth",
			env: map[string]string{
				"ALIYUNOSS_ENDPOINT": "http://test.com/",
				"ALIYUNOSS_SDK_AUTH": "not-a-bool",
			},
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{
				"ALIYUNOSS_ENDPOINT",
				"ALIYUNOSS_BUCKET",
				"ALIYUNOSS_ACCESS_KEY_ID",
				"ALIYUNOSS_ACCESS_KEY_SECRET",
				"ALIYUNOSS_SDK_AUTH",
			} {
				t.Setenv(k, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got, err := testBucketConfigFromEnv()
			if tt.wantErr {
				testutil.NotOk(t, err, "Expected error, got none")
				return
			}
			testutil.Ok(t, err, "Expected no error, got: %v", err)
			testutil.Equals(t, tt.want, got)
		})
	}
}
