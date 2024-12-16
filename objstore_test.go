// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/atomic"
)

func TestMetricBucket_Close(t *testing.T) {
	bkt := WrapWithMetrics(NewInMemBucket(), nil, "abc")
	// Expected initialized metrics.
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.ops))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.opsDuration))

	AcceptanceTest(t, bkt.WithExpectedErrs(bkt.IsObjNotFoundErr))
	testutil.Equals(t, float64(9), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(9), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.opsDuration))
	lastUpload := promtest.ToFloat64(bkt.metrics.lastSuccessfulUploadTime)
	testutil.Assert(t, lastUpload > 0, "last upload not greater than 0, val: %f", lastUpload)

	// Clear bucket, but don't clear metrics to ensure we use same.
	bkt.bkt = NewInMemBucket()
	AcceptanceTest(t, bkt)
	testutil.Equals(t, float64(18), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(18), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.metrics.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpIter)))
	// Not expected not found error here.
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpAttributes)))
	// Not expected not found errors, this should increment failure metric on get for not found as well, so +2.
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.metrics.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.metrics.opsDuration))
	testutil.Assert(t, promtest.ToFloat64(bkt.metrics.lastSuccessfulUploadTime) > lastUpload)
}

// TestMetricBucket_MultipleClients tests that the metrics from two different buckets clients are not conflicting with each other.
func TestMetricBucket_Multiple_Clients(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	WrapWithMetrics(NewInMemBucket(), reg, "abc")
	WrapWithMetrics(NewInMemBucket(), reg, "def")
}

func TestMetricBucket_UploadShouldPreserveReaderFeatures(t *testing.T) {
	tests := map[string]struct {
		reader             io.Reader
		expectedIsSeeker   bool
		expectedIsReaderAt bool
	}{
		"bytes.Reader": {
			reader:             bytes.NewReader([]byte("1")),
			expectedIsSeeker:   true,
			expectedIsReaderAt: true,
		},
		"bytes.Buffer": {
			reader:             bytes.NewBuffer([]byte("1")),
			expectedIsSeeker:   false,
			expectedIsReaderAt: false,
		},
		"os.File": {
			reader: func() io.Reader {
				// Create a test file.
				testFilepath := filepath.Join(t.TempDir(), "test")
				testutil.Ok(t, os.WriteFile(testFilepath, []byte("test"), os.ModePerm))

				// Open the file (it will be used as io.Reader).
				file, err := os.Open(testFilepath)
				testutil.Ok(t, err)
				t.Cleanup(func() {
					testutil.Ok(t, file.Close())
				})

				return file
			}(),
			expectedIsSeeker:   true,
			expectedIsReaderAt: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var uploadReader io.Reader

			m := &mockBucket{
				Bucket: WrapWithMetrics(NewInMemBucket(), nil, ""),
				upload: func(ctx context.Context, name string, r io.Reader) error {
					uploadReader = r
					return nil
				},
			}

			testutil.Ok(t, m.Upload(context.Background(), "dir/obj1", testData.reader))

			_, isSeeker := uploadReader.(io.Seeker)
			testutil.Equals(t, testData.expectedIsSeeker, isSeeker)

			_, isReaderAt := uploadReader.(io.ReaderAt)
			testutil.Equals(t, testData.expectedIsReaderAt, isReaderAt)
		})
	}
}

func TestMetricBucket_ReaderClose(t *testing.T) {
	const objPath = "dir/obj1"

	t.Run("Upload() should not close the input Reader", func(t *testing.T) {
		closeCalled := false

		reader := &mockReader{
			Reader: bytes.NewBuffer([]byte("test")),
			close: func() error {
				closeCalled = true
				return nil
			},
		}

		bucket := WrapWithMetrics(NewInMemBucket(), nil, "")
		testutil.Ok(t, bucket.Upload(context.Background(), objPath, reader))

		// Should not call Close() on the reader.
		testutil.Assert(t, !closeCalled)

		// An explicit call to Close() should close it.
		testutil.Ok(t, reader.Close())
		testutil.Assert(t, closeCalled)

		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.ops.WithLabelValues(OpUpload)))
		testutil.Equals(t, float64(0), promtest.ToFloat64(bucket.metrics.opsFailures.WithLabelValues(OpUpload)))
	})

	t.Run("Get() should return a wrapper io.ReadCloser that correctly Close the wrapped one", func(t *testing.T) {
		closeCalled := false

		origReader := &mockReader{
			Reader: bytes.NewBuffer([]byte("test")),
			close: func() error {
				closeCalled = true
				return nil
			},
		}

		bucket := WrapWithMetrics(&mockBucket{
			get: func(_ context.Context, _ string) (io.ReadCloser, error) {
				return origReader, nil
			},
		}, nil, "")

		wrappedReader, err := bucket.Get(context.Background(), objPath)
		testutil.Ok(t, err)
		testutil.Assert(t, origReader != wrappedReader)

		// Calling Close() to the wrappedReader should close origReader.
		testutil.Assert(t, !closeCalled)
		testutil.Ok(t, wrappedReader.Close())
		testutil.Assert(t, closeCalled)

		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.ops.WithLabelValues(OpGet)))
		testutil.Equals(t, float64(0), promtest.ToFloat64(bucket.metrics.opsFailures.WithLabelValues(OpGet)))
	})

	t.Run("GetRange() should return a wrapper io.ReadCloser that correctly Close the wrapped one", func(t *testing.T) {
		closeCalled := false

		origReader := &mockReader{
			Reader: bytes.NewBuffer([]byte("test")),
			close: func() error {
				closeCalled = true
				return nil
			},
		}

		bucket := WrapWithMetrics(&mockBucket{
			getRange: func(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
				return origReader, nil
			},
		}, nil, "")

		wrappedReader, err := bucket.GetRange(context.Background(), objPath, 0, 1)
		testutil.Ok(t, err)
		testutil.Assert(t, origReader != wrappedReader)

		// Calling Close() to the wrappedReader should close origReader.
		testutil.Assert(t, !closeCalled)
		testutil.Ok(t, wrappedReader.Close())
		testutil.Assert(t, closeCalled)

		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.ops.WithLabelValues(OpGetRange)))
		testutil.Equals(t, float64(0), promtest.ToFloat64(bucket.metrics.opsFailures.WithLabelValues(OpGetRange)))
	})
}

func TestMetricBucket_ReaderCloseError(t *testing.T) {
	origReader := &mockReader{
		Reader: bytes.NewBuffer([]byte("test")),
		close: func() error {
			return errors.New("mocked error")
		},
	}

	t.Run("Get() should track failure if reader Close() returns error", func(t *testing.T) {
		bucket := WrapWithMetrics(&mockBucket{
			get: func(ctx context.Context, name string) (io.ReadCloser, error) {
				return origReader, nil
			},
		}, nil, "")

		testutil.NotOk(t, bucket.Upload(context.Background(), "test", origReader))

		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.ops.WithLabelValues(OpUpload)))
		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.opsFailures.WithLabelValues(OpUpload)))
	})

	t.Run("Get() should track failure if reader Close() returns error", func(t *testing.T) {
		bucket := WrapWithMetrics(&mockBucket{
			get: func(ctx context.Context, name string) (io.ReadCloser, error) {
				return origReader, nil
			},
		}, nil, "")

		reader, err := bucket.Get(context.Background(), "test")
		testutil.Ok(t, err)
		testutil.NotOk(t, reader.Close())
		testutil.NotOk(t, reader.Close()) // Called twice to ensure metrics are not tracked twice.

		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.ops.WithLabelValues(OpGet)))
		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.opsFailures.WithLabelValues(OpGet)))
	})

	t.Run("GetRange() should track failure if reader Close() returns error", func(t *testing.T) {
		bucket := WrapWithMetrics(&mockBucket{
			getRange: func(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
				return origReader, nil
			},
		}, nil, "")

		reader, err := bucket.GetRange(context.Background(), "test", 0, 1)
		testutil.Ok(t, err)
		testutil.NotOk(t, reader.Close())
		testutil.NotOk(t, reader.Close()) // Called twice to ensure metrics are not tracked twice.

		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.ops.WithLabelValues(OpGetRange)))
		testutil.Equals(t, float64(1), promtest.ToFloat64(bucket.metrics.opsFailures.WithLabelValues(OpGetRange)))
	})
}

func TestDownloadUploadDirConcurrency(t *testing.T) {
	r := prometheus.NewRegistry()
	m := WrapWithMetrics(NewInMemBucket(), r, "")
	tempDir := t.TempDir()

	testutil.Ok(t, m.Upload(context.Background(), "dir/obj1", bytes.NewReader([]byte("1"))))
	testutil.Ok(t, m.Upload(context.Background(), "dir/obj2", bytes.NewReader([]byte("2"))))
	testutil.Ok(t, m.Upload(context.Background(), "dir/obj3", bytes.NewReader(bytes.Repeat([]byte("3"), 1024*1024))))

	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE objstore_bucket_operations_total counter
        objstore_bucket_operations_total{bucket="",operation="attributes"} 0
        objstore_bucket_operations_total{bucket="",operation="delete"} 0
        objstore_bucket_operations_total{bucket="",operation="exists"} 0
        objstore_bucket_operations_total{bucket="",operation="get"} 0
        objstore_bucket_operations_total{bucket="",operation="get_range"} 0
        objstore_bucket_operations_total{bucket="",operation="iter"} 0
        objstore_bucket_operations_total{bucket="",operation="upload"} 3
		`), `objstore_bucket_operations_total`))

	testutil.Ok(t, DownloadDir(context.Background(), log.NewNopLogger(), m, "dir/", "dir/", tempDir, WithFetchConcurrency(10)))
	i, err := os.ReadDir(tempDir)
	testutil.Ok(t, err)
	testutil.Assert(t, len(i) == 3)
	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE objstore_bucket_operations_total counter
        objstore_bucket_operations_total{bucket="",operation="attributes"} 0
        objstore_bucket_operations_total{bucket="",operation="delete"} 0
        objstore_bucket_operations_total{bucket="",operation="exists"} 0
        objstore_bucket_operations_total{bucket="",operation="get"} 3
        objstore_bucket_operations_total{bucket="",operation="get_range"} 0
        objstore_bucket_operations_total{bucket="",operation="iter"} 1
        objstore_bucket_operations_total{bucket="",operation="upload"} 3
		`), `objstore_bucket_operations_total`))

	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
        # HELP objstore_bucket_operation_fetched_bytes_total Total number of bytes fetched from bucket, per operation.
        # TYPE objstore_bucket_operation_fetched_bytes_total counter
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="attributes"} 0
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="delete"} 0
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="exists"} 0
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="get"} 1.048578e+06
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="get_range"} 0
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="iter"} 0
        objstore_bucket_operation_fetched_bytes_total{bucket="",operation="upload"} 0
		`), `objstore_bucket_operation_fetched_bytes_total`))

	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
        # HELP objstore_bucket_operation_transferred_bytes Number of bytes transferred from/to bucket per operation.
        # TYPE objstore_bucket_operation_transferred_bytes histogram
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="32768"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="65536"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="131072"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="262144"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="524288"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="1.048576e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="2.097152e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="4.194304e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="8.388608e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="1.6777216e+07"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="3.3554432e+07"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="6.7108864e+07"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="1.34217728e+08"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="2.68435456e+08"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="5.36870912e+08"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="1.073741824e+09"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get",le="+Inf"} 3
        objstore_bucket_operation_transferred_bytes_sum{bucket="",operation="get"} 1.048578e+06
        objstore_bucket_operation_transferred_bytes_count{bucket="",operation="get"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="32768"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="65536"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="131072"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="262144"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="524288"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="1.048576e+06"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="2.097152e+06"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="4.194304e+06"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="8.388608e+06"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="1.6777216e+07"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="3.3554432e+07"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="6.7108864e+07"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="1.34217728e+08"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="2.68435456e+08"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="5.36870912e+08"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="1.073741824e+09"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="get_range",le="+Inf"} 0
        objstore_bucket_operation_transferred_bytes_sum{bucket="",operation="get_range"} 0
        objstore_bucket_operation_transferred_bytes_count{bucket="",operation="get_range"} 0
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="32768"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="65536"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="131072"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="262144"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="524288"} 2
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="1.048576e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="2.097152e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="4.194304e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="8.388608e+06"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="1.6777216e+07"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="3.3554432e+07"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="6.7108864e+07"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="1.34217728e+08"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="2.68435456e+08"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="5.36870912e+08"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="1.073741824e+09"} 3
        objstore_bucket_operation_transferred_bytes_bucket{bucket="",operation="upload",le="+Inf"} 3
        objstore_bucket_operation_transferred_bytes_sum{bucket="",operation="upload"} 1.048578e+06
        objstore_bucket_operation_transferred_bytes_count{bucket="",operation="upload"} 3
		`), `objstore_bucket_operation_transferred_bytes`))

	testutil.Ok(t, UploadDir(context.Background(), log.NewNopLogger(), m, tempDir, "/dir-copy", WithUploadConcurrency(10)))

	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE objstore_bucket_operations_total counter
        objstore_bucket_operations_total{bucket="",operation="attributes"} 0
        objstore_bucket_operations_total{bucket="",operation="delete"} 0
        objstore_bucket_operations_total{bucket="",operation="exists"} 0
        objstore_bucket_operations_total{bucket="",operation="get"} 3
        objstore_bucket_operations_total{bucket="",operation="get_range"} 0
        objstore_bucket_operations_total{bucket="",operation="iter"} 1
        objstore_bucket_operations_total{bucket="",operation="upload"} 6
		`), `objstore_bucket_operations_total`))
}

func TestTimingReader(t *testing.T) {
	m := WrapWithMetrics(NewInMemBucket(), nil, "")
	r := bytes.NewReader([]byte("hello world"))
	tr := newTimingReader(time.Now(), r, true, OpGet, m.metrics.opsDuration, m.metrics.opsFailures, func(err error) bool {
		return false
	}, m.metrics.opsFetchedBytes, m.metrics.opsTransferredBytes)

	size, err := TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)

	smallBuf := make([]byte, 4)
	n, err := io.ReadFull(tr, smallBuf)
	testutil.Ok(t, err)
	testutil.Equals(t, 4, n)

	// Verify that size is still the same, after reading 4 bytes.
	size, err = TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)

	// Given the reader was bytes.Reader it should both implement io.Seeker and io.ReaderAt.
	_, isSeeker := tr.(io.Seeker)
	testutil.Assert(t, isSeeker)

	_, isReaderAt := tr.(io.ReaderAt)
	testutil.Assert(t, isReaderAt)

	testutil.Equals(t, float64(0), promtest.ToFloat64(m.metrics.opsFailures.WithLabelValues(OpGet)))
}

func TestTimingReader_ExpectedError(t *testing.T) {
	readerErr := errors.New("something went wrong")

	m := WrapWithMetrics(NewInMemBucket(), nil, "")
	r := dummyReader{readerErr}
	tr := newTimingReader(time.Now(), r, true, OpGet, m.metrics.opsDuration, m.metrics.opsFailures, func(err error) bool { return errors.Is(err, readerErr) }, m.metrics.opsFetchedBytes, m.metrics.opsTransferredBytes)

	buf := make([]byte, 1)
	_, err := io.ReadFull(tr, buf)
	testutil.Equals(t, readerErr, err)

	testutil.Equals(t, float64(0), promtest.ToFloat64(m.metrics.opsFailures.WithLabelValues(OpGet)))
}

func TestTimingReader_UnexpectedError(t *testing.T) {
	readerErr := errors.New("something went wrong")

	m := WrapWithMetrics(NewInMemBucket(), nil, "")
	r := dummyReader{readerErr}
	tr := newTimingReader(time.Now(), r, true, OpGet, m.metrics.opsDuration, m.metrics.opsFailures, func(err error) bool { return false }, m.metrics.opsFetchedBytes, m.metrics.opsTransferredBytes)

	buf := make([]byte, 1)
	_, err := io.ReadFull(tr, buf)
	testutil.Equals(t, readerErr, err)

	testutil.Equals(t, float64(1), promtest.ToFloat64(m.metrics.opsFailures.WithLabelValues(OpGet)))
}

func TestTimingReader_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := WrapWithMetrics(NewInMemBucket(), nil, "")
	r := dummyReader{ctx.Err()}
	tr := newTimingReader(time.Now(), r, true, OpGet, m.metrics.opsDuration, m.metrics.opsFailures, func(err error) bool { return false }, m.metrics.opsFetchedBytes, m.metrics.opsTransferredBytes)

	buf := make([]byte, 1)
	_, err := io.ReadFull(tr, buf)
	testutil.Equals(t, ctx.Err(), err)

	testutil.Equals(t, float64(0), promtest.ToFloat64(m.metrics.opsFailures.WithLabelValues(OpGet)))
}

type dummyReader struct {
	err error
}

func (r dummyReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func TestTimingReader_ShouldCorrectlyWrapFile(t *testing.T) {
	// Create a test file.
	testFilepath := filepath.Join(t.TempDir(), "test")
	testutil.Ok(t, os.WriteFile(testFilepath, []byte("test"), os.ModePerm))

	// Open the file (it will be used as io.Reader).
	file, err := os.Open(testFilepath)
	testutil.Ok(t, err)
	t.Cleanup(func() {
		testutil.Ok(t, file.Close())
	})

	m := WrapWithMetrics(NewInMemBucket(), nil, "")
	r := newTimingReader(time.Now(), file, true, "", m.metrics.opsDuration, m.metrics.opsFailures, func(err error) bool {
		return false
	}, m.metrics.opsFetchedBytes, m.metrics.opsTransferredBytes)

	// It must both implement io.Seeker and io.ReaderAt.
	_, isSeeker := r.(io.Seeker)
	testutil.Assert(t, isSeeker)

	_, isReaderAt := r.(io.ReaderAt)
	testutil.Assert(t, isReaderAt)
}

func TestDownloadDir_CleanUp(t *testing.T) {
	b := unreliableBucket{
		Bucket:  NewInMemBucket(),
		n:       3,
		current: atomic.NewInt32(0),
	}
	tempDir := t.TempDir()

	testutil.Ok(t, b.Upload(context.Background(), "dir/obj1", bytes.NewReader([]byte("1"))))
	testutil.Ok(t, b.Upload(context.Background(), "dir/obj2", bytes.NewReader([]byte("2"))))
	testutil.Ok(t, b.Upload(context.Background(), "dir/obj3", bytes.NewReader([]byte("3"))))

	// We exapect the third Get to fail
	testutil.NotOk(t, DownloadDir(context.Background(), log.NewNopLogger(), b, "dir/", "dir/", tempDir))
	_, err := os.Stat(tempDir)
	testutil.Assert(t, os.IsNotExist(err))
}

// unreliableBucket implements Bucket and returns an error on every n-th Get.
type unreliableBucket struct {
	Bucket

	n       int32
	current *atomic.Int32
}

func (b unreliableBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.current.Inc()%b.n == 0 {
		return nil, errors.Errorf("some error message")
	}
	return b.Bucket.Get(ctx, name)
}

// mockReader implements io.ReadCloser and allows to mock the functions.
type mockReader struct {
	io.Reader

	close func() error
}

func (r *mockReader) Close() error {
	if r.close != nil {
		return r.close()
	}
	return nil
}

// mockBucket implements Bucket and allows to mock the functions.
type mockBucket struct {
	Bucket

	upload   func(ctx context.Context, name string, r io.Reader) error
	get      func(ctx context.Context, name string) (io.ReadCloser, error)
	getRange func(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)
}

func (b *mockBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	if b.upload != nil {
		return b.upload(ctx, name, r)
	}
	return errors.New("Upload has not been mocked")
}

func (b *mockBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.get != nil {
		return b.get(ctx, name)
	}
	return nil, errors.New("Get has not been mocked")
}

func (b *mockBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if b.getRange != nil {
		return b.getRange(ctx, name, off, length)
	}
	return nil, errors.New("GetRange has not been mocked")
}

func Test_TryToGetSizeLimitedReader(t *testing.T) {
	b := &bytes.Buffer{}
	r := io.LimitReader(b, 1024)
	size, err := TryToGetSize(r)
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1024), size)
}
