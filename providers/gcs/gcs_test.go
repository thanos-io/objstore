// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package gcs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/fullstorydev/emulators/storage/gcsemu"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore/errutil"
	"google.golang.org/api/option"
)

func TestBucket_Get_ShouldReturnErrorIfServerTruncateResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
		w.Header().Set("Content-Length", "100")

		// Write less bytes than the content length.
		_, err := w.Write([]byte("12345"))
		testutil.Ok(t, err)
	}))
	defer srv.Close()

	os.Setenv("STORAGE_EMULATOR_HOST", srv.Listener.Addr().String())

	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
	}

	// NewBucketWithConfig wraps newBucket and processes HTTP options. Can skip for test.
	bkt, err := newBucket(context.Background(), log.NewNopLogger(), cfg, []option.ClientOption{})
	testutil.Ok(t, err)

	reader, err := bkt.Get(context.Background(), "test")
	testutil.Ok(t, err)

	// We expect an error when reading back.
	_, err = io.ReadAll(reader)
	testutil.NotOk(t, err)
	testutil.Equals(t, "storage: partial request not satisfied", err.Error())
}

func TestNewBucketWithConfig_ShouldCreateGRPC(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)
	err = os.Setenv("GCS_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)
	err = os.Setenv("STORAGE_EMULATOR_HOST_GRPC", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)

	// Check if the bucket is created.
	testutil.Assert(t, bkt != nil, "expected bucket to be created")
}

func TestParseConfig_ChunkSize(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		assertions func(cfg Config)
	}{
		{
			name:  "DefaultConfig",
			input: `bucket: abcd`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.ChunkSizeBytes, 0)
			},
		},
		{
			name: "CustomConfig",
			input: `bucket: abcd
chunk_size_bytes: 1024`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.ChunkSizeBytes, 1024)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig([]byte(tc.input))
			testutil.Ok(t, err)
			tc.assertions(cfg)
		})
	}
}

func TestParseConfig_HTTPConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		assertions func(cfg Config)
	}{
		{
			name:  "DefaultHTTPConfig",
			input: `bucket: abcd`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.HTTPConfig.IdleConnTimeout, model.Duration(90*time.Second))
				testutil.Equals(t, cfg.HTTPConfig.ResponseHeaderTimeout, model.Duration(2*time.Minute))
				testutil.Equals(t, cfg.HTTPConfig.InsecureSkipVerify, false)
			},
		},
		{
			name: "CustomHTTPConfig",
			input: `bucket: abcd
http_config:
  insecure_skip_verify: true
  idle_conn_timeout: 50s
  response_header_timeout: 1m`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.HTTPConfig.IdleConnTimeout, model.Duration(50*time.Second))
				testutil.Equals(t, cfg.HTTPConfig.ResponseHeaderTimeout, model.Duration(1*time.Minute))
				testutil.Equals(t, cfg.HTTPConfig.InsecureSkipVerify, true)
			},
		},
		{
			name: "CustomHTTPConfigWithTLS",
			input: `bucket: abcd
http_config:
  tls_config:
    ca_file: /certs/ca.crt
    cert_file: /certs/cert.crt
    key_file: /certs/key.key
    server_name: server
    insecure_skip_verify: false`,
			assertions: func(cfg Config) {
				testutil.Equals(t, "/certs/ca.crt", cfg.HTTPConfig.TLSConfig.CAFile)
				testutil.Equals(t, "/certs/cert.crt", cfg.HTTPConfig.TLSConfig.CertFile)
				testutil.Equals(t, "/certs/key.key", cfg.HTTPConfig.TLSConfig.KeyFile)
				testutil.Equals(t, "server", cfg.HTTPConfig.TLSConfig.ServerName)
				testutil.Equals(t, false, cfg.HTTPConfig.TLSConfig.InsecureSkipVerify)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig([]byte(tc.input))
			testutil.Ok(t, err)
			tc.assertions(cfg)
		})
	}
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}
	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", errutil.WrapWithErrRoundtripper)
	testutil.Ok(t, err)
	_, err = bkt.Get(context.Background(), "test-bucket")
	testutil.NotOk(t, err)
	testutil.Assert(t, errutil.IsMockedError(err), "Expected RoundTripper error, got: %v", err)
}
