// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package s3_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	"github.com/thanos-io/objstore/exthttp"
	"github.com/thanos-io/objstore/providers/s3"
	"github.com/thanos-io/objstore/test/e2e/e2ethanos"
)

// Regression benchmark for https://github.com/thanos-io/thanos/issues/3917 and https://github.com/thanos-io/thanos/issues/3967.
//
//	$ export ver=v1 && go test ./pkg/objstore/s3/... -run '^$' -bench '^BenchmarkUpload' -benchtime 5s -count 5 \
//		-memprofile=${ver}.mem.pprof -cpuprofile=${ver}.cpu.pprof | tee ${ver}.txt .
func BenchmarkUpload(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	e, err := e2e.NewDockerEnvironment("e2e_bench_mino_client", e2e.WithLogger(log.NewNopLogger()))
	testutil.Ok(b, err)
	b.Cleanup(e2ethanos.CleanScenario(b, e))

	const bucket = "benchmark"
	m := e2ethanos.NewMinio(e, "benchmark", bucket)
	testutil.Ok(b, e2e.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(
		log.NewNopLogger(),
		e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()),
		"test-feed",
		nil,
	)
	testutil.Ok(b, err)

	buf := bytes.Buffer{}
	buf.Grow(1028 * 1028 * 100) // 100MB.
	word := "abcdefghij"
	for i := 0; i < buf.Cap()/len(word); i++ {
		_, _ = buf.WriteString(word)
	}
	str := buf.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testutil.Ok(b, bkt.Upload(ctx, "test", strings.NewReader(str)))
	}
}

func TestSSECencryption(t *testing.T) {
	ctx := context.Background()
	e, err := e2e.NewDockerEnvironment("e2e-ssec", e2e.WithLogger(log.NewNopLogger()))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "sse-c-encryption"
	m := e2ethanos.NewMinio(e, "sse-c-encryption", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	cfg := s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.Endpoint("https"),
		Insecure:  false,
		HTTPConfig: exthttp.HTTPConfig{
			TLSConfig: exthttp.TLSConfig{
				CAFile:   filepath.Join(m.Dir(), "certs", "CAs", "ca.crt"),
				CertFile: filepath.Join(m.Dir(), "certs", "public.crt"),
				KeyFile:  filepath.Join(m.Dir(), "certs", "private.key"),
			},
		},
		SSEConfig: s3.SSEConfig{
			Type:          string(encrypt.SSEC),
			EncryptionKey: "testdata/encryption_key",
		},
		BucketLookupType: s3.AutoLookup,
	}

	bkt, err := s3.NewBucketWithConfig(
		log.NewNopLogger(),
		cfg,
		"test-ssec",
		nil,
	)
	testutil.Ok(t, err)

	upload := "secret content"
	testutil.Ok(t, bkt.Upload(ctx, "encrypted", strings.NewReader(upload)))

	exists, err := bkt.Exists(ctx, "encrypted")
	testutil.Ok(t, err)
	if !exists {
		t.Fatalf("upload failed")
	}

	r, err := bkt.Get(ctx, "encrypted")
	testutil.Ok(t, err)
	b, err := io.ReadAll(r)
	testutil.Ok(t, err)
	testutil.Equals(t, upload, string(b))
}
