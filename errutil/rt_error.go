package errutil

import (
	"net/http"

	"github.com/pkg/errors"
)

var rtErr = errors.New("RoundTripper error")

func IsMockedError(err error) bool {
	return errors.Is(err, rtErr)
}

// ErrorRoundTripper is a custom RoundTripper that always returns an error.
type ErrorRoundTripper struct {
	Err error
}

func (ert *ErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, ert.Err
}

func WrapWithErrRoundtripper(rt http.RoundTripper) http.RoundTripper {
	return &ErrorRoundTripper{Err: rtErr}
}
