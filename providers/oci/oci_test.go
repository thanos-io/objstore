package oci

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/errutil"
	"gopkg.in/yaml.v2"
	"time"
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

	require.ElementsMatch(
		t,
		[]objstore.ObjectUploadOptionType{
			objstore.ContentType,
			objstore.IfMatch,
			objstore.IfNotExists,
		},
		b.SupportedObjectUploadOptions(),
	)
}
func TestApplyUploadConditionsIfMatch(t *testing.T) {
	version := &objstore.ObjectVersion{
		Type:  objstore.ETag,
		Value: `"test-etag"`,
	}

	uploadOptions := objstore.ApplyObjectUploadOptions(
		objstore.WithIfMatch(version),
	)

	req := transfer.UploadRequest{}

	err := applyUploadConditions(&req, uploadOptions)

	require.NoError(t, err)
	require.NotNil(t, req.IfMatch)
	require.Equal(t, `"test-etag"`, *req.IfMatch)
	require.Nil(t, req.IfNoneMatch)
}

func TestApplyUploadConditionsIfNotExists(t *testing.T) {
	uploadOptions := objstore.ApplyObjectUploadOptions(
		objstore.WithIfNotExists(),
	)

	req := transfer.UploadRequest{}

	err := applyUploadConditions(&req, uploadOptions)

	require.NoError(t, err)
	require.Nil(t, req.IfMatch)
	require.NotNil(t, req.IfNoneMatch)
	require.Equal(t, "*", *req.IfNoneMatch)
}

func TestApplyUploadConditionsWithoutCondition(t *testing.T) {
	uploadOptions := objstore.ApplyObjectUploadOptions()

	req := transfer.UploadRequest{}

	err := applyUploadConditions(&req, uploadOptions)

	require.NoError(t, err)
	require.Nil(t, req.IfMatch)
	require.Nil(t, req.IfNoneMatch)
}

func TestApplyUploadConditionsRejectsGeneration(t *testing.T) {
	version := &objstore.ObjectVersion{
		Type:  objstore.Generation,
		Value: "123",
	}

	uploadOptions := objstore.ApplyObjectUploadOptions(
		objstore.WithIfMatch(version),
	)

	req := transfer.UploadRequest{}

	err := applyUploadConditions(&req, uploadOptions)

	require.ErrorIs(t, err, errConditionInvalid)
	require.Nil(t, req.IfMatch)
	require.Nil(t, req.IfNoneMatch)
}

func TestAttributesFromHeadObjectResponse(t *testing.T) {
	lastModified := common.SDKTime{
		Time: time.Date(
			2026,
			time.August,
			4,
			12,
			0,
			0,
			0,
			time.UTC,
		),
	}

	response := objectstorage.HeadObjectResponse{
		ContentLength: common.Int64(1234),
		LastModified:  &lastModified,
		ETag:          common.String(`"oci-etag-value"`),
	}

	attrs := attributesFromHeadObjectResponse(response)

	require.Equal(t, int64(1234), attrs.Size)
	require.Equal(t, lastModified.Time, attrs.LastModified)

	require.NotNil(t, attrs.Version)
	require.Equal(t, objstore.ETag, attrs.Version.Type)
	require.Equal(t, `"oci-etag-value"`, attrs.Version.Value)
}
func TestAttributesFromHeadObjectResponseWithoutETag(t *testing.T) {
	response := objectstorage.HeadObjectResponse{
		ContentLength: common.Int64(10),
	}

	attrs := attributesFromHeadObjectResponse(response)

	require.Equal(t, int64(10), attrs.Size)
	require.Nil(t, attrs.Version)
}
