// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package s3

import (
	"context"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

// DefaultAWSSDKAuthTimeout is the default deadline for a single AWS SDK credential retrieval.
const DefaultAWSSDKAuthTimeout = 30 * time.Second

// AWSSDKAuth retrieves credentials from the aws-sdk-go.
type AWSSDKAuth struct {
	Region string
	// Timeout bounds each retrieval. Zero means DefaultAWSSDKAuthTimeout.
	Timeout time.Duration
	creds   aws.Credentials
}

// NewAWSSDKAuth returns a pointer to a new Credentials object
// wrapping the environment variable provider.
func NewAWSSDKAuth(region string) *credentials.Credentials {
	return credentials.New(&AWSSDKAuth{
		Region:  region,
		Timeout: DefaultAWSSDKAuthTimeout,
	})
}

func (a *AWSSDKAuth) RetrieveWithCredContext(cc *credentials.CredContext) (credentials.Value, error) {
	timeout := a.Timeout
	if timeout <= 0 {
		timeout = DefaultAWSSDKAuthTimeout
	}
	// minio-go runs retrieval detached from the caller's context and makes other callers wait on it,
	// so bound it here; CredContext carries no context to derive from.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(a.Region))
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "load AWS SDK config")
	}

	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "retrieve AWS SDK credentials")
	}

	a.creds = creds

	return credentials.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		SignerType:      credentials.SignatureV4,
	}, nil
}

// Retrieve retrieves the keys from the environment.
func (a *AWSSDKAuth) Retrieve() (credentials.Value, error) {
	return a.RetrieveWithCredContext(nil)
}

// IsExpired returns if the credentials have been retrieved.
func (a *AWSSDKAuth) IsExpired() bool {
	return a.creds.Expired()
}
