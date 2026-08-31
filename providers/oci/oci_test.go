package oci

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/errutil"
	"gopkg.in/yaml.v2"
)

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	const mockPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDCFENGw33yGihy92pDjZQhl0C36rPJj+CvfSC8+q28hxA161QF
NUd13wuCTUcq0Qd2qsBe/2hFyc2DCJJg0h1L78+6Z4UMR7EOcpfdUE9Hf3m/hs+F
UR45uBJeDK1HSFHD8bHKD6kv8FPGfJTotc+2xjJwoYi+1hqp1fIekaxsyQIDAQAB
AoGBAJR8ZkCUvx5kzv+utdl7T5MnordT1TvoXXJGXK7ZZ+UuvMNUCdN2QPc4sBiA
QWvLw1cSKt5DsKZ8UETpYPy8pPYnnDEz2dDYiaew9+xEpubyeW2oH4Zx71wqBtOK
kqwrXa/pzdpiucRRjk6vE6YY7EBBs/g7uanVpGibOVAEsqH1AkEA7DkjVH28WDUg
f1nqvfn2Kj6CT7nIcE3jGJsZZ7zlZmBmHFDONMLUrXR/Zm3pR5m0tCmBqa5RK95u
412jt1dPIwJBANJT3v8pnkth48bQo/fKel6uEYyboRtA5/uHuHkZ6FQF7OUkGogc
mSJluOdc5t6hI1VsLn0QZEjQZMEOWr+wKSMCQQCC4kXJEsHAve77oP6HtG/IiEn7
kpyUXRNvFsDE0czpJJBvL/aRFUJxuRK91jhjC68sA7NsKMGg5OXb5I5Jj36xAkEA
gIT7aFOYBFwGgQAQkWNKLvySgKbAZRTeLBacpHMuQdl1DfdntvAyqpAZ0lY0RKmW
G6aFKaqQfOXKCyWoUiVknQJAXrlgySFci/2ueKlIE1QqIiLSZ8V8OlpFLRnb1pzI
7U1yQXnTAEFYM560yJlzUpOb1V4cScGd365tiSMvxLOvTA==
-----END RSA PRIVATE KEY-----`

	config := DefaultConfig
	config.Provider = "raw"
	config.Tenancy = "test"
	config.User = "test"
	config.Region = "test"
	config.Fingerprint = "123"
	config.PrivateKey = mockPrivateKey
	config.Passphrase = "123"
	ociConfig, err := yaml.Marshal(config)
	testutil.Ok(t, err)

	_, err = NewBucket(log.NewNopLogger(), ociConfig, errutil.WrapWithErrRoundtripper)
	// We expect an error from the RoundTripper
	testutil.NotOk(t, err)
	testutil.Assert(t, errutil.IsMockedError(err), "Expected RoundTripper error, got: %v", err)
}

func TestSupportedObjectUploadOptions(t *testing.T) {
	b := &Bucket{}
	testutil.Equals(t, []objstore.ObjectUploadOptionType{
		objstore.ContentType,
		objstore.IfMatch,
		objstore.IfNotExists,
	}, b.SupportedObjectUploadOptions())
}

func TestApplyUploadConditions(t *testing.T) {
	t.Run("if match", func(t *testing.T) {
		version := &objstore.ObjectVersion{Type: objstore.ETag, Value: `"test-etag"`}
		opts := objstore.ApplyObjectUploadOptions(objstore.WithIfMatch(version))
		req := transfer.UploadRequest{}

		testutil.Ok(t, applyUploadConditions(&req, opts))
		testutil.Equals(t, `"test-etag"`, *req.IfMatch)
		testutil.Assert(t, req.IfNoneMatch == nil)
	})

	t.Run("if not exists", func(t *testing.T) {
		opts := objstore.ApplyObjectUploadOptions(objstore.WithIfNotExists())
		req := transfer.UploadRequest{}

		testutil.Ok(t, applyUploadConditions(&req, opts))
		testutil.Assert(t, req.IfMatch == nil)
		testutil.Equals(t, "*", *req.IfNoneMatch)
	})

	t.Run("rejects generation", func(t *testing.T) {
		version := &objstore.ObjectVersion{Type: objstore.Generation, Value: "123"}
		opts := objstore.ApplyObjectUploadOptions(objstore.WithIfMatch(version))
		req := transfer.UploadRequest{}

		testutil.Equals(t, errConditionInvalid, applyUploadConditions(&req, opts))
		testutil.Assert(t, req.IfMatch == nil)
		testutil.Assert(t, req.IfNoneMatch == nil)
	})
}

func TestAttributesFromHeadObjectResponse(t *testing.T) {
	lastModified := common.SDKTime{Time: time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)}
	response := objectstorage.HeadObjectResponse{
		ContentLength: common.Int64(1234),
		LastModified:  &lastModified,
		ETag:          common.String(`"oci-etag-value"`),
	}

	attrs := attributesFromHeadObjectResponse(response)
	testutil.Equals(t, int64(1234), attrs.Size)
	testutil.Equals(t, lastModified.Time, attrs.LastModified)
	testutil.Assert(t, attrs.Version != nil)
	testutil.Equals(t, objstore.ETag, attrs.Version.Type)
	testutil.Equals(t, `"oci-etag-value"`, attrs.Version.Value)
}

func TestAttributesFromHeadObjectResponseWithoutETag(t *testing.T) {
	attrs := attributesFromHeadObjectResponse(objectstorage.HeadObjectResponse{
		ContentLength: common.Int64(10),
	})
	testutil.Equals(t, int64(10), attrs.Size)
	testutil.Assert(t, attrs.Version == nil)
}

type fakeServiceError struct {
	statusCode int
}

func (e fakeServiceError) Error() string           { return "fake OCI service error" }
func (e fakeServiceError) GetHTTPStatusCode() int  { return e.statusCode }
func (e fakeServiceError) GetMessage() string      { return "fake" }
func (e fakeServiceError) GetCode() string         { return "Fake" }
func (e fakeServiceError) GetOpcRequestID() string { return "" }

func TestIsConditionNotMetErr(t *testing.T) {
	b := &Bucket{}
	testutil.Assert(t, b.IsConditionNotMetErr(fakeServiceError{statusCode: http.StatusPreconditionFailed}))
	testutil.Assert(t, !b.IsConditionNotMetErr(fakeServiceError{statusCode: http.StatusNotFound}))
	testutil.Assert(t, b.IsConditionNotMetErr(errConditionInvalid))
	testutil.Assert(t, !b.IsConditionNotMetErr(errors.New("unrelated error")))
}

func TestCopyConditionalUpload(t *testing.T) {
	t.Run("enforces PutObject size limit", func(t *testing.T) {
		var dst bytes.Buffer
		size, err := copyConditionalUpload(context.Background(), &dst, strings.NewReader("12345"), 4)
		testutil.Equals(t, int64(5), size)
		testutil.Assert(t, errors.Is(err, errConditionalUploadTooLarge))
	})

	t.Run("honors context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := copyConditionalUpload(ctx, io.Discard, strings.NewReader("data"), 4)
		testutil.Assert(t, errors.Is(err, context.Canceled))
	})

	t.Run("preserves reader errors", func(t *testing.T) {
		wantErr := errors.New("reader failed")
		_, err := copyConditionalUpload(context.Background(), io.Discard, errorReader{err: wantErr}, 4)
		testutil.Assert(t, errors.Is(err, wantErr))
	})
}

type errorReader struct {
	err error
}

func (r errorReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func TestConditionalPutObjectBodyCanBeRetried(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "conditional-upload-*")
	testutil.Ok(t, err)
	defer func() { _ = tmp.Close() }()

	want := []byte("complete object body")
	_, err = tmp.Write(want)
	testutil.Ok(t, err)
	_, err = tmp.Seek(0, io.SeekStart)
	testutil.Ok(t, err)

	req := conditionalPutObjectRequest(transfer.UploadRequest{}, tmp, int64(len(want)))
	var attempts [][]byte
	policy := common.NewRetryPolicy(
		2,
		func(_ common.OCIOperationResponse) bool {
			return len(attempts) == 1
		},
		func(_ common.OCIOperationResponse) time.Duration { return 0 },
	)

	operation := common.OCIOperation(func(
		_ context.Context,
		_ common.OCIRequest,
		body *common.OCIReadSeekCloser,
		_ map[string]string,
	) (common.OCIResponse, error) {
		data, readErr := io.ReadAll(body)
		testutil.Ok(t, readErr)
		attempts = append(attempts, data)
		if len(attempts) == 1 {
			return nil, errors.New("transient upload failure")
		}
		return nil, nil
	})

	_, err = common.Retry(context.Background(), req, operation, policy)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(attempts))
	testutil.Equals(t, want, attempts[0])
	testutil.Equals(t, want, attempts[1])
}
