package oss

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestGetRangeUnspecifiedLength(t *testing.T) {
	const payload = "@test-data@"

	var gotRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		http.ServeContent(w, r, "obj_1.some", time.Unix(0, 0), strings.NewReader(payload))
	}))
	t.Cleanup(srv.Close)

	bkt, err := NewBucketWithConfig(log.NewNopLogger(), Config{
		Endpoint:        srv.URL,
		Bucket:          "test",
		AccessKeyID:     "id",
		AccessKeySecret: "secret",
	}, "test", nil)
	testutil.Ok(t, err)

	rc, err := bkt.GetRange(context.Background(), "id1/obj_1.some", 1, -1)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc.Close()) }()

	body, err := io.ReadAll(rc)
	testutil.Ok(t, err)

	testutil.Equals(t, "bytes=1-", gotRange)
	testutil.Equals(t, payload[1:], string(body))
}
