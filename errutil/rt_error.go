package errutil

import (
	"net/http"

	"github.com/pkg/errors"
)

var Rt_err = errors.New("RoundTripper error")

// ErrorRoundTripper is a custom RoundTripper that always returns an error.
type ErrorRoundTripper struct {
	Err error
}

func (ert *ErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, ert.Err
}

func WrapRoundtripper(rt http.RoundTripper) http.RoundTripper {
	return &ErrorRoundTripper{Err: Rt_err}
}
