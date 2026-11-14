package oss

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
)

func TestSdkAuth(t *testing.T) {
	ak := "test-access-key-id"
	os.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", ak)
	os.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "test-access-key-secret")
	defer func() {
		os.Unsetenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
		os.Unsetenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	}()

	logger := log.NewNopLogger()
	var gotReq *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	ts := server.Client().Transport

	b, err := NewBucketWithConfig(logger, Config{
		Endpoint: server.URL,
		Bucket:   "foobar",
		SdkAuth:  true,
	}, "", func(rt http.RoundTripper) http.RoundTripper {
		return ts
	})
	testutil.Ok(t, err, "Failed to create bucket with SDK auth")

	data, err := b.Get(context.TODO(), "test.txt")
	testutil.NotOk(t, err, "Expected error when accessing bucket with fake server")
	testutil.Assert(t, data == nil, "Expected no data, got: %v", data)

	testutil.Assert(t, gotReq != nil, "Expected request to be made, but got nil")
	testutil.Assert(t, len(gotReq.Header["Authorization"]) > 0,
		"Expected Authorization header to be set, but it was not")

	testutil.Assert(t, strings.Contains(gotReq.Header["Authorization"][0], ak),
		"Expected Authorization header to contain access key ID, but it did not: %s",
		gotReq.Header["Authorization"][0])
}
