package oss

import (
	"context"
	"net/http"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

// ErrorRoundTripper is a custom RoundTripper that always returns an error
type ErrorRoundTripper struct {
	Err error
}

func (ert *ErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, ert.Err
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {

	var config Config
	config.Endpoint = "http://test.com/"
	config.AccessKeyID = "123"
	config.AccessKeySecret = "123"
	config.Bucket = "test"
	config.AccessKeySecret = "123"

	rt := &ErrorRoundTripper{Err: errors.New("RoundTripper error")}

	bkt, err := NewBucketWithConfig(log.NewNopLogger(), config, "test", rt)
	// We expect an error from the RoundTripper
	testutil.Ok(t, err)
	_, err = bkt.Get(context.Background(), "test")
	testutil.NotOk(t, err)
	testutil.Assert(t, errors.Is(err, rt.Err), "Expected RoundTripper error, got: %v", err)
}
