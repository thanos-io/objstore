package oss

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/thanos-io/objstore/errutil"
)

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

// recordedRequest captures one HTTP request seen by the fake OSS endpoint.
type recordedRequest struct {
	method     string
	query      string
	contentLen int64
	bodyLen    int64
	body       []byte // Only recorded for small bodies (e.g. CompleteMultipartUpload XML).
}

// recordingRoundTripper answers the multipart calls the OSS SDK makes, without
// any live Aliyun endpoint. It records every request so tests can assert the
// exact call sequence an upload produces.
type recordingRoundTripper struct {
	requests  []recordedRequest
	failParts bool // Fail UploadPart calls with a 500.
}

func (rt *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var bodyLen int64
	var body []byte
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		bodyLen = int64(len(b))
		if len(b) < 1<<20 {
			body = b
		}
		_ = req.Body.Close()
	}
	q := req.URL.Query()
	rt.requests = append(rt.requests, recordedRequest{
		method:     req.Method,
		query:      req.URL.RawQuery,
		contentLen: req.ContentLength,
		bodyLen:    bodyLen,
		body:       body,
	})

	resp := func(status int, header http.Header, body string) *http.Response {
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     header,
		}
	}

	switch {
	case req.Method == http.MethodPost && q.Has("uploads"):
		// InitiateMultipartUpload.
		return resp(http.StatusOK, make(http.Header),
			`<InitiateMultipartUploadResult><Bucket>test</Bucket><Key>test-object</Key><UploadId>test-upload-id</UploadId></InitiateMultipartUploadResult>`), nil
	case req.Method == http.MethodPut && q.Has("partNumber"):
		// UploadPart.
		if rt.failParts {
			return resp(http.StatusInternalServerError, make(http.Header),
				`<Error><Code>InternalError</Code><Message>boom</Message><RequestId>test</RequestId></Error>`), nil
		}
		header := make(http.Header)
		header.Set("ETag", fmt.Sprintf(`"etag-%s"`, q.Get("partNumber")))
		return resp(http.StatusOK, header, ""), nil
	case req.Method == http.MethodPost && q.Has("uploadId"):
		// CompleteMultipartUpload.
		return resp(http.StatusOK, make(http.Header),
			`<CompleteMultipartUploadResult><Location>http://test.com/test/test-object</Location><Bucket>test</Bucket><ETag>"final-etag"</ETag><Key>test-object</Key></CompleteMultipartUploadResult>`), nil
	case req.Method == http.MethodDelete && q.Has("uploadId"):
		// AbortMultipartUpload.
		return resp(http.StatusNoContent, make(http.Header), ""), nil
	case req.Method == http.MethodPut:
		// PutObject.
		return resp(http.StatusOK, make(http.Header), ""), nil
	default:
		return resp(http.StatusNotFound, make(http.Header), ""), nil
	}
}

// unknownSizeReader hides the size of the underlying reader, so
// objstore.TryToGetSize errors on it — the same situation as a pipe, a gzip
// stream or a chunked HTTP body.
type unknownSizeReader struct {
	io.Reader
}

func newRecordingTestBucket(t *testing.T, rt http.RoundTripper) *Bucket {
	t.Helper()
	config := Config{
		Endpoint:        "http://test.com/",
		AccessKeyID:     "123",
		AccessKeySecret: "123",
		Bucket:          "test",
	}
	bkt, err := NewBucketWithConfig(log.NewNopLogger(), config, "test", func(http.RoundTripper) http.RoundTripper { return rt })
	testutil.Ok(t, err)
	return bkt
}

func TestUploadUnknownSizeSinglePart(t *testing.T) {
	rt := &recordingRoundTripper{}
	bkt := newRecordingTestBucket(t, rt)

	data := bytes.Repeat([]byte("a"), 1024)
	testutil.Ok(t, bkt.Upload(context.Background(), "test-object", unknownSizeReader{bytes.NewReader(data)}))

	testutil.Equals(t, 3, len(rt.requests))
	testutil.Equals(t, http.MethodPost, rt.requests[0].method)
	testutil.Assert(t, strings.Contains(rt.requests[0].query, "uploads"), "expected initiate multipart upload, got query %q", rt.requests[0].query)
	testutil.Equals(t, http.MethodPut, rt.requests[1].method)
	testutil.Assert(t, strings.Contains(rt.requests[1].query, "partNumber=1"), "expected part 1 upload, got query %q", rt.requests[1].query)
	testutil.Equals(t, int64(1024), rt.requests[1].contentLen)
	testutil.Equals(t, int64(1024), rt.requests[1].bodyLen)
	testutil.Equals(t, http.MethodPost, rt.requests[2].method)
	testutil.Assert(t, strings.Contains(rt.requests[2].query, "uploadId=test-upload-id"), "expected complete multipart upload, got query %q", rt.requests[2].query)
	testutil.Assert(t, strings.Contains(string(rt.requests[2].body), "<PartNumber>1</PartNumber>"), "expected complete body to list part 1, got %q", string(rt.requests[2].body))
}

func TestUploadUnknownSizeMultiPart(t *testing.T) {
	rt := &recordingRoundTripper{}
	bkt := newRecordingTestBucket(t, rt)

	data := bytes.Repeat([]byte("b"), PartSize+10)
	testutil.Ok(t, bkt.Upload(context.Background(), "test-object", unknownSizeReader{bytes.NewReader(data)}))

	testutil.Equals(t, 4, len(rt.requests))
	testutil.Assert(t, strings.Contains(rt.requests[0].query, "uploads"), "expected initiate multipart upload, got query %q", rt.requests[0].query)
	testutil.Assert(t, strings.Contains(rt.requests[1].query, "partNumber=1"), "expected part 1 upload, got query %q", rt.requests[1].query)
	testutil.Equals(t, int64(PartSize), rt.requests[1].contentLen)
	testutil.Equals(t, int64(PartSize), rt.requests[1].bodyLen)
	testutil.Assert(t, strings.Contains(rt.requests[2].query, "partNumber=2"), "expected part 2 upload, got query %q", rt.requests[2].query)
	testutil.Equals(t, int64(10), rt.requests[2].contentLen)
	testutil.Equals(t, int64(10), rt.requests[2].bodyLen)
	testutil.Assert(t, strings.Contains(rt.requests[3].query, "uploadId=test-upload-id"), "expected complete multipart upload, got query %q", rt.requests[3].query)
	testutil.Assert(t, strings.Contains(string(rt.requests[3].body), "<PartNumber>1</PartNumber>"), "expected complete body to list part 1, got %q", string(rt.requests[3].body))
	testutil.Assert(t, strings.Contains(string(rt.requests[3].body), "<PartNumber>2</PartNumber>"), "expected complete body to list part 2, got %q", string(rt.requests[3].body))
}

func TestUploadUnknownSizeEmpty(t *testing.T) {
	rt := &recordingRoundTripper{}
	bkt := newRecordingTestBucket(t, rt)

	testutil.Ok(t, bkt.Upload(context.Background(), "test-object", unknownSizeReader{bytes.NewReader(nil)}))

	// An empty stream goes through a single PutObject; no multipart upload
	// is initiated (zero parts is invalid).
	testutil.Equals(t, 1, len(rt.requests))
	testutil.Equals(t, http.MethodPut, rt.requests[0].method)
	testutil.Equals(t, "", rt.requests[0].query)
	testutil.Equals(t, int64(0), rt.requests[0].bodyLen)
}

func TestUploadUnknownSizePartFailureAborts(t *testing.T) {
	rt := &recordingRoundTripper{failParts: true}
	bkt := newRecordingTestBucket(t, rt)

	data := bytes.Repeat([]byte("c"), 1024)
	err := bkt.Upload(context.Background(), "test-object", unknownSizeReader{bytes.NewReader(data)})
	testutil.NotOk(t, err)

	// Initiate, failed part upload, then abort; no complete.
	testutil.Equals(t, 3, len(rt.requests))
	testutil.Assert(t, strings.Contains(rt.requests[0].query, "uploads"), "expected initiate multipart upload, got query %q", rt.requests[0].query)
	testutil.Assert(t, strings.Contains(rt.requests[1].query, "partNumber=1"), "expected part 1 upload, got query %q", rt.requests[1].query)
	testutil.Equals(t, http.MethodDelete, rt.requests[2].method)
	testutil.Assert(t, strings.Contains(rt.requests[2].query, "uploadId=test-upload-id"), "expected abort multipart upload, got query %q", rt.requests[2].query)
}

func TestUploadKnownSizeUnchanged(t *testing.T) {
	rt := &recordingRoundTripper{}
	bkt := newRecordingTestBucket(t, rt)

	// A *bytes.Reader has a known size, so Upload keeps the existing
	// single-PutObject path and never touches multipart.
	data := bytes.Repeat([]byte("d"), 1024)
	testutil.Ok(t, bkt.Upload(context.Background(), "test-object", bytes.NewReader(data)))

	testutil.Equals(t, 1, len(rt.requests))
	testutil.Equals(t, http.MethodPut, rt.requests[0].method)
	testutil.Equals(t, "", rt.requests[0].query)
	testutil.Equals(t, int64(1024), rt.requests[0].bodyLen)
}
