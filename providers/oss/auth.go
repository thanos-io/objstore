// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package oss

import (
	"sync"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

type Credentials struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

type CredentialsProvider struct {
	// mu serializes accesses to cred: the credentials-go providers are not
	// safe for concurrent use and would otherwise also let every in-flight
	// request refresh the credentials at the same time.
	mu     sync.Mutex
	cred   credentials.Credential
	logger log.Logger
}

// GetCredentialsE is used by the OSS client while signing requests, so
// returning an error prevents the request from being sent with empty
// credentials and losing the root cause.
func (p *CredentialsProvider) GetCredentialsE() (oss.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cred, err := p.cred.GetCredential()
	if err != nil {
		return nil, errors.Wrap(err, "get aliyun credentials")
	}

	return &Credentials{
		AccessKeyId:     tea.StringValue(cred.AccessKeyId),
		AccessKeySecret: tea.StringValue(cred.AccessKeySecret),
		SecurityToken:   tea.StringValue(cred.SecurityToken),
	}, nil
}

var _ oss.CredentialsProviderE = &CredentialsProvider{}

// GetCredentials is kept for the OSS client paths that cannot return an error
// (e.g. signing RTMP URLs), so it can only log the failure.
func (p *CredentialsProvider) GetCredentials() oss.Credentials {
	cred, err := p.GetCredentialsE()
	if err != nil {
		level.Error(p.logger).Log("msg", "get credentials failed", "err", err)
		return &Credentials{}
	}

	return cred
}

func NewCredentialsProvider(logger log.Logger) (*CredentialsProvider, error) {
	cred, err := credentials.NewCredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "create new credentials")
	}

	return &CredentialsProvider{
		cred:   cred,
		logger: logger,
	}, nil
}

func (c *Credentials) GetAccessKeyID() string {
	return c.AccessKeyId
}

func (c *Credentials) GetAccessKeySecret() string {
	return c.AccessKeySecret
}

func (c *Credentials) GetSecurityToken() string {
	return c.SecurityToken
}
