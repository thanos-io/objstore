// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package oss

import (
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
	cred   credentials.Credential
	logger log.Logger
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

func (p *CredentialsProvider) GetCredentials() oss.Credentials {
	cred, err := p.cred.GetCredential()
	if err != nil {
		level.Error(p.logger).Log("msg", "get credentials failed", "err", err)
		return &Credentials{}
	}

	return &Credentials{
		AccessKeyId:     tea.StringValue(cred.AccessKeyId),
		AccessKeySecret: tea.StringValue(cred.AccessKeySecret),
		SecurityToken:   tea.StringValue(cred.SecurityToken),
	}
}
