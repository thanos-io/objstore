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
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))

	AcceptanceTest(t, bkt.WithExpectedErrs(bkt.IsObjNotFoundErr))
	testutil.Equals(t, float64(9), promtest.ToFloat64(bkt.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(9), promtest.ToFloat64(bkt.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))
	lastUpload := promtest.ToFloat64(bkt.lastSuccessfulUploadTime)
	testutil.Assert(t, lastUpload > 0, "last upload not greater than 0, val: %f", lastUpload)

	// Clear bucket, but don't clear metrics to ensure we use same.
	bkt.bkt = NewInMemBucket()
	AcceptanceTest(t, bkt)
	testutil.Equals(t, float64(18), promtest.ToFloat64(bkt.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(18), promtest.ToFloat64(bkt.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpIter)))
	// Not expected not found error here.
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpAttributes)))
	// Not expected not found errors, this should increment failure metric on get for not found as well, so +2.
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))
	testutil.Assert(t, promtest.ToFloat64(bkt.lastSuccessfulUploadTime) > lastUpload)
}

// TestMetricBucket_MultipleClients tests that the metrics from two different buckets clients are not conflicting with each other.
func TestMetricBucket_Multiple_Clients(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	WrapWithMetrics(NewInMemBucket(), reg, "abc")
	WrapWithMetrics(NewInMemBucket(), reg, "def")
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
	tr := newTimingReader(r, true, "", m.opsDuration, m.opsFailures, func(err error) bool {
		return false
	}, m.opsFetchedBytes, m.opsTransferredBytes)

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
	r := newTimingReader(file, true, "", m.opsDuration, m.opsFailures, func(err error) bool {
		return false
	}, m.opsFetchedBytes, m.opsTransferredBytes)

	// It must both implement io.Seeker and io.ReaderAt.
	_, isSeeker := r.(io.Seeker)
	testutil.Assert(t, isSeeker)

	_, isReaderAt := r.(io.ReaderAt)
	testutil.Assert(t, isReaderAt)
}

func TestWrapWithMetrics_UploadShouldPreserveReaderFeatures(t *testing.T) {
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
			m := &uploadTrackerTestBucket{
				Bucket: WrapWithMetrics(NewInMemBucket(), nil, ""),
			}

			testutil.Ok(t, m.Upload(context.Background(), "dir/obj1", testData.reader))

			_, isSeeker := m.uploadReader.(io.Seeker)
			testutil.Equals(t, testData.expectedIsSeeker, isSeeker)

			_, isReaderAt := m.uploadReader.(io.ReaderAt)
			testutil.Equals(t, testData.expectedIsReaderAt, isReaderAt)
		})
	}
}

func TestWrapWithMetrics_UploadShouldNotCloseTheReader(t *testing.T) {
	reader := &closeTrackerReader{
		Reader: bytes.NewBuffer([]byte("test")),
	}

	bucket := WrapWithMetrics(NewInMemBucket(), nil, "")
	testutil.Ok(t, bucket.Upload(context.Background(), "dir/obj1", reader))

	// Should not call Close() on the reader.
	testutil.Assert(t, !reader.closeCalled)
}

// seekerBucket implements Bucket and keeps a reference of the io.Reader passed to Upload().
type uploadTrackerTestBucket struct {
	Bucket

	uploadReader io.Reader
}

func (b *uploadTrackerTestBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	b.uploadReader = r
	return nil
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

// closeTrackerReader is a io.ReadCloser which keeps track whether Close() has been called.
type closeTrackerReader struct {
	io.Reader
	closeCalled bool
}

func (r *closeTrackerReader) Close() error {
	r.closeCalled = true
	return nil
}
