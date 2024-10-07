package errutil

import "net/http"

// ErrorRoundTripper is a custom RoundTripper that always returns an error.
type ErrorRoundTripper struct {
	Err error
}

func (ert *ErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, ert.Err
}
