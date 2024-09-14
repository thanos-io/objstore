package obs

import (
	"net/http"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

// ErrorRoundTripper is a custom RoundTripper that always returns an error
type ErrorRoundTripper struct {
	Err       error
	Transport *http.Transport
}

func (ert *ErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, ert.Err
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	config := DefaultConfig
	config.Endpoint = "https://test.com"
	config.SecretKey = "test123"
	config.AccessKey = "test123"

	rt := &ErrorRoundTripper{Err: errors.New("RoundTripper error")}
	_, err := NewBucketWithConfig(log.NewNopLogger(), config, rt)
	// We expect an error from the RoundTripper
	testutil.NotOk(t, err)
	testutil.Assert(t, errors.Is(err, rt.Err), "Expected RoundTripper error, got: %v", err)
}
