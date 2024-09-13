// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package swift

import (
	"net/http"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

func TestParseConfig(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
username: thanos
user_domain_name: userDomain
project_name: thanosProject
project_domain_name: projectDomain`)

	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	testutil.Equals(t, "http://identity.something.com/v3", cfg.AuthUrl)
	testutil.Equals(t, "thanos", cfg.Username)
	testutil.Equals(t, "userDomain", cfg.UserDomainName)
	testutil.Equals(t, "thanosProject", cfg.ProjectName)
	testutil.Equals(t, "projectDomain", cfg.ProjectDomainName)
}

func TestParseConfigFail(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
tenant_name: something`)

	_, err := parseConfig(input)
	// Must result in unmarshal error as there's no `tenant_name` in SwiftConfig.
	testutil.NotOk(t, err)
}

func TestParseConfig_HTTPConfig(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
username: thanos
user_domain_name: userDomain
project_name: thanosProject
project_domain_name: projectDomain
http_config:
  tls_config:
    ca_file: /certs/ca.crt
    cert_file: /certs/cert.crt
    key_file: /certs/key.key
    server_name: server
    insecure_skip_verify: false`)
	cfg, err := parseConfig([]byte(input))

	testutil.Ok(t, err)

	testutil.Equals(t, "http://identity.something.com/v3", cfg.AuthUrl)
	testutil.Equals(t, "thanos", cfg.Username)
	testutil.Equals(t, "userDomain", cfg.UserDomainName)
	testutil.Equals(t, "thanosProject", cfg.ProjectName)
	testutil.Equals(t, "projectDomain", cfg.ProjectDomainName)
	testutil.Equals(t, model.Duration(90*time.Second), cfg.HTTPConfig.IdleConnTimeout)
	testutil.Equals(t, model.Duration(2*time.Minute), cfg.HTTPConfig.ResponseHeaderTimeout)
	testutil.Equals(t, false, cfg.HTTPConfig.InsecureSkipVerify)

}

// ErrorRoundTripper is a custom RoundTripper that always returns an error
type ErrorRoundTripper struct {
	Err error
}

func (ert *ErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, ert.Err
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	logger := log.NewNopLogger()
	rt := &ErrorRoundTripper{Err: errors.New("RoundTripper error")}
	config := DefaultConfig
	config.AuthUrl = "http://identity.something.com/v3"
	_, err := NewContainerFromConfig(logger, &config, false, rt)

	// We expect an error from the RoundTripper
	testutil.NotOk(t, err)
	testutil.Assert(t, errors.Is(err, rt.Err), "Expected RoundTripper error, got: %v", err)
}
