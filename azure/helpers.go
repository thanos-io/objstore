// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/thanos-io/objstore"
)

func getContainerClient(conf Config) (*azblob.ContainerClient, error) {
	dt, err := defaultTransport(conf)
	if err != nil {
		return nil, err
	}
	opt := &azblob.ClientOptions{
		Retry: policy.RetryOptions{
			MaxRetries:    conf.PipelineConfig.MaxTries,
			TryTimeout:    time.Duration(conf.PipelineConfig.TryTimeout),
			RetryDelay:    time.Duration(conf.PipelineConfig.RetryDelay),
			MaxRetryDelay: time.Duration(conf.PipelineConfig.MaxRetryDelay),
		},
		Telemetry: policy.TelemetryOptions{
			ApplicationID: "Thanos",
		},
		Transport: &http.Client{Transport: dt},
	}
	containerURL := fmt.Sprintf("https://%s.%s/%s", conf.StorageAccountName, conf.Endpoint, conf.ContainerName)

	// Use shared keys if set
	if conf.StorageAccountKey != "" {
		cred, err := azblob.NewSharedKeyCredential(conf.StorageAccountName, conf.StorageAccountKey)
		if err != nil {
			return nil, err
		}
		containerClient, err := azblob.NewContainerClientWithSharedKey(containerURL, cred, nil)
		if err != nil {
			return nil, err
		}
		return containerClient, nil
	}

	// Use msi for authentication
	msiOpt := &azidentity.ManagedIdentityCredentialOptions{}
	if conf.UserAssignedID != "" {
		msiOpt.ID = azidentity.ClientID(conf.UserAssignedID)
	}
	cred, err := azidentity.NewManagedIdentityCredential(msiOpt)
	if err != nil {
		return nil, err
	}
	containerClient, err := azblob.NewContainerClient(containerURL, cred, opt)
	if err != nil {
		return nil, err
	}
	return containerClient, nil
}

func defaultTransport(config Config) (*http.Transport, error) {
	tlsConfig, err := objstore.NewTLSConfig(&config.HTTPConfig.TLSConfig)
	if err != nil {
		return nil, err
	}
	if config.HTTPConfig.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,

		MaxIdleConns:          config.HTTPConfig.MaxIdleConns,
		MaxIdleConnsPerHost:   config.HTTPConfig.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(config.HTTPConfig.IdleConnTimeout),
		MaxConnsPerHost:       config.HTTPConfig.MaxConnsPerHost,
		TLSHandshakeTimeout:   time.Duration(config.HTTPConfig.TLSHandshakeTimeout),
		ExpectContinueTimeout: time.Duration(config.HTTPConfig.ExpectContinueTimeout),

		ResponseHeaderTimeout: time.Duration(config.HTTPConfig.ResponseHeaderTimeout),
		DisableCompression:    config.HTTPConfig.DisableCompression,
		TLSClientConfig:       tlsConfig,
	}, nil
}
