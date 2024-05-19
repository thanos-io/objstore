// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package s3 implements common object storage abstractions against s3-compatible APIs.
package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/cristalhq/hedgedhttp"
	"github.com/efficientgo/core/logerrcapture"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/pkg/errors"
	"github.com/prometheus/common/version"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/exthttp"
)

type ctxKey int

type BucketLookupType int

func (blt BucketLookupType) String() string {
	return []string{"auto", "virtual-hosted", "path"}[blt]
}

func (blt BucketLookupType) MinioType() minio.BucketLookupType {
	return []minio.BucketLookupType{
		minio.BucketLookupAuto,
		minio.BucketLookupDNS,
		minio.BucketLookupPath,
	}[blt]
}

func (blt BucketLookupType) MarshalYAML() (interface{}, error) {
	return blt.String(), nil
}

func (blt *BucketLookupType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var lookupType string
	if err := unmarshal(&lookupType); err != nil {
		return err
	}

	switch lookupType {
	case "auto":
		*blt = AutoLookup
		return nil
	case "virtual-hosted":
		*blt = VirtualHostLookup
		return nil
	case "path":
		*blt = PathLookup
		return nil
	}

	return fmt.Errorf("unsupported bucket lookup type: %s", lookupType)
}

const (
	AutoLookup BucketLookupType = iota
	VirtualHostLookup
	PathLookup

	// DirDelim is the delimiter used to model a directory structure in an object store bucket.
	DirDelim = "/"

	// SSEKMS is the name of the SSE-KMS method for objectstore encryption.
	SSEKMS = "SSE-KMS"

	// SSEC is the name of the SSE-C method for objstore encryption.
	SSEC = "SSE-C"

	// SSES3 is the name of the SSE-S3 method for objstore encryption.
	SSES3 = "SSE-S3"

	// sseConfigKey is the context key to override SSE config. This feature is used by downstream
	// projects (eg. Cortex) to inject custom SSE config on a per-request basis. Future work or
	// refactoring can introduce breaking changes as far as the functionality is preserved.
	// NOTE: we're using a context value only because it's a very specific S3 option. If SSE will
	// be available to wider set of backends we should probably add a variadic option to Get() and Upload().
	sseConfigKey = ctxKey(0)

	// Storage class header.
	amzStorageClass = "X-Amz-Storage-Class"
)

var DefaultConfig = Config{
	PutUserMetadata:  map[string]string{},
	HTTPConfig:       exthttp.DefaultHTTPConfig,
	DisableMultipart: false,
	PartSize:         1024 * 1024 * 64, // 64MB.
	BucketLookupType: AutoLookup,
	SendContentMd5:   true, // Default to using MD5.
}

// HTTPConfig exists here only because Cortex depends on it, and we depend on Cortex.
// Deprecated.
// TODO(bwplotka): Remove it, once we remove Cortex cycle dep, or Cortex stops using this.
type HTTPConfig = exthttp.HTTPConfig

// Config stores the configuration for s3 bucket.
type Config struct {
	Bucket             string             `yaml:"bucket"`
	Endpoint           string             `yaml:"endpoint"`
	Region             string             `yaml:"region"`
	AWSSDKAuth         bool               `yaml:"aws_sdk_auth"`
	AccessKey          string             `yaml:"access_key"`
	Insecure           bool               `yaml:"insecure"`
	SignatureV2        bool               `yaml:"signature_version2"`
	SecretKey          string             `yaml:"secret_key"`
	SessionToken       string             `yaml:"session_token"`
	PutUserMetadata    map[string]string  `yaml:"put_user_metadata"`
	HTTPConfig         exthttp.HTTPConfig `yaml:"http_config"`
	TraceConfig        TraceConfig        `yaml:"trace"`
	ListObjectsVersion string             `yaml:"list_objects_version"`
	BucketLookupType   BucketLookupType   `yaml:"bucket_lookup_type"`
	SendContentMd5     bool               `yaml:"send_content_md5"`
	DisableMultipart   bool               `yaml:"disable_multipart"`
	// PartSize used for multipart upload. Only used if uploaded object size is known and larger than configured PartSize.
	// NOTE we need to make sure this number does not produce more parts than 10 000.
	PartSize    uint64    `yaml:"part_size"`
	SSEConfig   SSEConfig `yaml:"sse_config"`
	STSEndpoint string    `yaml:"sts_endpoint"`
}

// SSEConfig deals with the configuration of SSE for Minio. The following options are valid:
// KMSEncryptionContext == https://docs.aws.amazon.com/kms/latest/developerguide/services-s3.html#s3-encryption-context
type SSEConfig struct {
	Type                 string            `yaml:"type"`
	KMSKeyID             string            `yaml:"kms_key_id"`
	KMSEncryptionContext map[string]string `yaml:"kms_encryption_context"`
	EncryptionKey        string            `yaml:"encryption_key"`
}

type TraceConfig struct {
	Enable bool `yaml:"enable"`
}

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	logger           log.Logger
	name             string
	client           *minio.Client
	defaultSSE       encrypt.ServerSide
	putUserMetadata  map[string]string
	storageClass     string
	disableMultipart bool
	partSize         uint64
	listObjectsV1    bool
	sendContentMd5   bool
}

// parseConfig unmarshals a buffer into a Config with default values.
func parseConfig(conf []byte) (Config, error) {
	config := DefaultConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return Config{}, err
	}

	return config, nil
}

// NewBucket returns a new Bucket using the provided s3 config values.
func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewBucketWithConfig(logger, config, component)
}

type overrideSignerType struct {
	credentials.Provider
	signerType credentials.SignatureType
}

func (s *overrideSignerType) Retrieve() (credentials.Value, error) {
	v, err := s.Provider.Retrieve()
	if err != nil {
		return v, err
	}
	if !v.SignerType.IsAnonymous() {
		v.SignerType = s.signerType
	}
	return v, nil
}

// NewBucketWithConfig returns a new Bucket using the provided s3 config values.
func NewBucketWithConfig(logger log.Logger, config Config, component string) (*Bucket, error) {
	var chain []credentials.Provider

	// TODO(bwplotka): Don't do flags as they won't scale, use actual params like v2, v4 instead
	wrapCredentialsProvider := func(p credentials.Provider) credentials.Provider { return p }
	if config.SignatureV2 {
		wrapCredentialsProvider = func(p credentials.Provider) credentials.Provider {
			return &overrideSignerType{Provider: p, signerType: credentials.SignatureV2}
		}
	}

	if err := validate(config); err != nil {
		return nil, err
	}

	if config.AWSSDKAuth {
		chain = []credentials.Provider{
			wrapCredentialsProvider(&AWSSDKAuth{Region: config.Region}),
		}
	} else if config.AccessKey != "" {
		chain = []credentials.Provider{wrapCredentialsProvider(&credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     config.AccessKey,
				SecretAccessKey: config.SecretKey,
				SessionToken:    config.SessionToken,
				SignerType:      credentials.SignatureV4,
			},
		})}
	} else {
		hedgedClient, err := hedgedhttp.New(hedgedhttp.Config{
			Transport: http.DefaultTransport,
		})
		if err != nil {
			return nil, err
		}
		chain = []credentials.Provider{
			wrapCredentialsProvider(&credentials.EnvAWS{}),
			wrapCredentialsProvider(&credentials.FileAWSCredentials{}),
			wrapCredentialsProvider(&credentials.IAM{
				Client: &http.Client{
					Transport: hedgedClient,
				},
				Endpoint: config.STSEndpoint,
			}),
		}
	}

	// Check if a roundtripper has been set in the config
	// otherwise build the default transport.
	var rt http.RoundTripper
	if config.HTTPConfig.Transport != nil {
		rt = config.HTTPConfig.Transport
	} else {
		var err error
		rt, err = exthttp.DefaultTransport(config.HTTPConfig)
		if err != nil {
			return nil, err
		}
	}

	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:        credentials.NewChainCredentials(chain),
		Secure:       !config.Insecure,
		Region:       config.Region,
		Transport:    rt,
		BucketLookup: config.BucketLookupType.MinioType(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize s3 client")
	}
	client.SetAppInfo(fmt.Sprintf("thanos-%s", component), fmt.Sprintf("%s (%s)", version.Version, runtime.Version()))

	var sse encrypt.ServerSide
	if config.SSEConfig.Type != "" {
		switch config.SSEConfig.Type {
		case SSEKMS:
			// If the KMSEncryptionContext is a nil map the header that is
			// constructed by the encrypt.ServerSide object will be base64
			// encoded "nil" which is not accepted by AWS.
			if config.SSEConfig.KMSEncryptionContext == nil {
				config.SSEConfig.KMSEncryptionContext = make(map[string]string)
			}
			sse, err = encrypt.NewSSEKMS(config.SSEConfig.KMSKeyID, config.SSEConfig.KMSEncryptionContext)
			if err != nil {
				return nil, errors.Wrap(err, "initialize s3 client SSE-KMS")
			}

		case SSEC:
			key, err := os.ReadFile(config.SSEConfig.EncryptionKey)
			if err != nil {
				return nil, err
			}

			sse, err = encrypt.NewSSEC(key)
			if err != nil {
				return nil, errors.Wrap(err, "initialize s3 client SSE-C")
			}

		case SSES3:
			sse = encrypt.NewSSE()

		default:
			sseErrMsg := errors.Errorf("Unsupported type %q was provided. Supported types are SSE-S3, SSE-KMS, SSE-C", config.SSEConfig.Type)
			return nil, errors.Wrap(sseErrMsg, "Initialize s3 client SSE Config")
		}
	}

	if config.TraceConfig.Enable {
		logWriter := log.NewStdlibAdapter(level.Debug(logger), log.MessageKey("s3TraceMsg"))
		client.TraceOn(logWriter)
	}

	if config.ListObjectsVersion != "" && config.ListObjectsVersion != "v1" && config.ListObjectsVersion != "v2" {
		return nil, errors.Errorf("Initialize s3 client list objects version: Unsupported version %q was provided. Supported values are v1, v2", config.ListObjectsVersion)
	}

	var storageClass string
	amzStorageClassLower := strings.ToLower(amzStorageClass)
	for k, v := range config.PutUserMetadata {
		if strings.ToLower(k) == amzStorageClassLower {
			delete(config.PutUserMetadata, k)
			storageClass = v
			break
		}
	}

	bkt := &Bucket{
		logger:           logger,
		name:             config.Bucket,
		client:           client,
		defaultSSE:       sse,
		putUserMetadata:  config.PutUserMetadata,
		storageClass:     storageClass,
		disableMultipart: config.DisableMultipart,
		partSize:         config.PartSize,
		listObjectsV1:    config.ListObjectsVersion == "v1",
		sendContentMd5:   config.SendContentMd5,
	}
	return bkt, nil
}

// Name returns the bucket name for s3.
func (b *Bucket) Name() string {
	return b.name
}

// validate checks to see the config options are set.
func validate(conf Config) error {
	if conf.Endpoint == "" {
		return errors.New("no s3 endpoint in config file")
	}

	if conf.AWSSDKAuth && conf.AccessKey != "" {
		return errors.New("aws_sdk_auth and access_key are mutually exclusive configurations")
	}

	if conf.AccessKey == "" && conf.SecretKey != "" {
		return errors.New("no s3 access_key specified while secret_key is present in config file; either both should be present in config or envvars/IAM should be used.")
	}

	if conf.AccessKey != "" && conf.SecretKey == "" {
		return errors.New("no s3 secret_key specified while access_key is present in config file; either both should be present in config or envvars/IAM should be used.")
	}

	if conf.SSEConfig.Type == SSEC && conf.SSEConfig.EncryptionKey == "" {
		return errors.New("encryption_key must be set if sse_config.type is set to 'SSE-C'")
	}

	if conf.SSEConfig.Type == SSEKMS && conf.SSEConfig.KMSKeyID == "" {
		return errors.New("kms_key_id must be set if sse_config.type is set to 'SSE-KMS'")
	}

	return nil
}

// ValidateForTests checks to see the config options for tests are set.
func ValidateForTests(conf Config) error {
	if conf.Endpoint == "" ||
		conf.AccessKey == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient s3 test configuration information")
	}
	return nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	opts := minio.ListObjectsOptions{
		Prefix:    dir,
		Recursive: objstore.ApplyIterOptions(options...).Recursive,
		UseV1:     b.listObjectsV1,
	}

	for object := range b.client.ListObjects(ctx, b.name, opts) {
		// Catch the error when failed to list objects.
		if object.Err != nil {
			return object.Err
		}
		// This sometimes happens with empty buckets.
		if object.Key == "" {
			continue
		}
		// The s3 client can also return the directory itself in the ListObjects call above.
		if object.Key == dir {
			continue
		}
		if err := f(object.Key); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	sse, err := b.getServerSideEncryption(ctx)
	if err != nil {
		return nil, err
	}

	opts := &minio.GetObjectOptions{ServerSideEncryption: sse}
	if length != -1 {
		if err := opts.SetRange(off, off+length-1); err != nil {
			return nil, err
		}
	} else if off > 0 {
		if err := opts.SetRange(off, 0); err != nil {
			return nil, err
		}
	}
	r, err := b.client.GetObject(ctx, b.name, name, *opts)
	if err != nil {
		return nil, err
	}

	// NotFoundObject error is revealed only after first Read. This does the initial GetRequest. Prefetch this here
	// for convenience.
	if _, err := r.Read(nil); err != nil {
		defer logerrcapture.Do(b.logger, r.Close, "s3 get range obj close")

		// First GET Object request error.
		return nil, err
	}

	return r, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := b.client.StatObject(ctx, b.name, name, minio.StatObjectOptions{})
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "stat s3 object")
	}

	return true, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	sse, err := b.getServerSideEncryption(ctx)
	if err != nil {
		return err
	}

	// TODO(https://github.com/thanos-io/thanos/issues/678): Remove guessing length when minio provider will support multipart upload without this.
	size, err := objstore.TryToGetSize(r)
	if err != nil {
		level.Warn(b.logger).Log("msg", "could not guess file size for multipart upload; upload might be not optimized", "name", name, "err", err)
		size = -1
	}

	partSize := b.partSize
	if size < int64(partSize) {
		partSize = 0
	}

	// Cloning map since minio may modify it
	userMetadata := make(map[string]string, len(b.putUserMetadata))
	for k, v := range b.putUserMetadata {
		userMetadata[k] = v
	}

	if _, err := b.client.PutObject(
		ctx,
		b.name,
		name,
		r,
		size,
		minio.PutObjectOptions{
			DisableMultipart:     b.disableMultipart,
			PartSize:             partSize,
			ServerSideEncryption: sse,
			UserMetadata:         userMetadata,
			StorageClass:         b.storageClass,
			SendContentMd5:       b.sendContentMd5,
			// 4 is what minio-go have as the default. To be certain we do micro benchmark before any changes we
			// ensure we pin this number to four.
			// TODO(bwplotka): Consider adjusting this number to GOMAXPROCS or to expose this in config if it becomes bottleneck.
			NumThreads: 4,
		},
	); err != nil {
		return errors.Wrap(err, "upload s3 object")
	}

	return nil
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	objInfo, err := b.client.StatObject(ctx, b.name, name, minio.StatObjectOptions{})
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	return objstore.ObjectAttributes{
		Size:         objInfo.Size,
		LastModified: objInfo.LastModified,
	}, nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	return b.client.RemoveObject(ctx, b.name, name, minio.RemoveObjectOptions{})
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return minio.ToErrorResponse(errors.Cause(err)).Code == "NoSuchKey"
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *Bucket) IsAccessDeniedErr(err error) bool {
	return minio.ToErrorResponse(errors.Cause(err)).Code == "AccessDenied"
}

func (b *Bucket) Close() error { return nil }

// getServerSideEncryption returns the SSE to use.
func (b *Bucket) getServerSideEncryption(ctx context.Context) (encrypt.ServerSide, error) {
	if value := ctx.Value(sseConfigKey); value != nil {
		if sse, ok := value.(encrypt.ServerSide); ok {
			return sse, nil
		}
		return nil, errors.New("invalid SSE config override provided in the context")
	}

	return b.defaultSSE, nil
}

func configFromEnv() Config {
	c := Config{
		Bucket:       os.Getenv("S3_BUCKET"),
		Endpoint:     os.Getenv("S3_ENDPOINT"),
		AccessKey:    os.Getenv("S3_ACCESS_KEY"),
		SecretKey:    os.Getenv("S3_SECRET_KEY"),
		SessionToken: os.Getenv("S3_SESSION_TOKEN"),
	}

	c.Insecure, _ = strconv.ParseBool(os.Getenv("S3_INSECURE"))
	c.HTTPConfig.InsecureSkipVerify, _ = strconv.ParseBool(os.Getenv("S3_INSECURE_SKIP_VERIFY"))
	c.SignatureV2, _ = strconv.ParseBool(os.Getenv("S3_SIGNATURE_VERSION2"))
	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := ValidateForTests(c); err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("S3_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset S3_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
			"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
			"to safety (accidentally pointing prod bucket for test) as well as aws s3 not being fully strong consistent.")
	}

	return NewTestBucketFromConfig(t, location, c, true)
}

func NewTestBucketFromConfig(t testing.TB, location string, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	ctx := context.Background()

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	bktToCreate := c.Bucket
	if c.Bucket != "" && reuseBucket {
		if err := b.Iter(ctx, "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "s3 check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "AWS bucket for AWS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		bktToCreate = objstore.CreateTemporaryTestBucketName(t)
	}

	if err := b.client.MakeBucket(ctx, bktToCreate, minio.MakeBucketOptions{Region: location}); err != nil {
		return nil, nil, err
	}
	b.name = bktToCreate
	t.Log("created temporary AWS bucket for AWS tests with name", bktToCreate, "in", location)

	return b, func() {
		objstore.EmptyBucket(t, ctx, b)
		if err := b.client.RemoveBucket(ctx, bktToCreate); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}

// ContextWithSSEConfig returns a context with a  custom SSE config set. The returned context should be
// provided to S3 objstore client functions to override the default SSE config.
func ContextWithSSEConfig(ctx context.Context, value encrypt.ServerSide) context.Context {
	return context.WithValue(ctx, sseConfigKey, value)
}
