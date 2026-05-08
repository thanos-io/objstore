// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package oss

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	aliossCred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	alicred "github.com/aliyun/credentials-go/credentials"
	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/exthttp"
)

// PartSize is a part size for multi part upload.
const PartSize = 1024 * 1024 * 128

// Config stores the configuration for oss bucket.
type Config struct {
	Endpoint        string `yaml:"endpoint"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`

	// RRSA (RAM Role for Service Account) related configurations
	RoleArn           string `yaml:"role_arn"`
	OIDCProviderArn   string `yaml:"oidc_provider_arn"`
	OIDCTokenFilePath string `yaml:"oidc_token_file_path"`
	RoleSessionName   string `yaml:"role_session_name"`
}

// Bucket implements the store.Bucket interface.
type Bucket struct {
	name   string
	logger log.Logger
	client *oss.Client
	config Config
}

func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := Config{
		Endpoint:        os.Getenv("ALIYUNOSS_ENDPOINT"),
		Bucket:          os.Getenv("ALIYUNOSS_BUCKET"),
		AccessKeyID:     os.Getenv("ALIYUNOSS_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("ALIYUNOSS_ACCESS_KEY_SECRET"),
	}

	if c.Endpoint == "" {
		return nil, nil, errors.New("aliyun oss endpoint is not present in config file")
	}
	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "true" {
		t.Log("ALIYUNOSS_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset ALIYUNOSS_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true.")
		return NewTestBucketFromConfig(t, c, true)
	}
	return NewTestBucketFromConfig(t, c, false)
}

func (b *Bucket) Provider() objstore.ObjProvider { return objstore.ALIYUNOSS }

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	size, err := objstore.TryToGetSize(r)
	if err != nil {
		return errors.Wrapf(err, "failed to get size apriori to upload %s", name)
	}

	uploadOpts := objstore.ApplyObjectUploadOptions(opts...)

	chunksnum, lastslice := int(math.Floor(float64(size)/PartSize)), size%PartSize

	ncloser := io.NopCloser(r)
	switch chunksnum {
	case 0:
		req := &oss.PutObjectRequest{
			Bucket: oss.Ptr(b.name),
			Key:    oss.Ptr(name),
			Body:   ncloser,
		}
		if uploadOpts.ContentType != "" {
			req.ContentType = oss.Ptr(uploadOpts.ContentType)
		}
		if _, err := b.client.PutObject(ctx, req); err != nil {
			return errors.Wrap(err, "failed to upload oss object")
		}
	default:
		{
			initReq := &oss.InitiateMultipartUploadRequest{
				Bucket: oss.Ptr(b.name),
				Key:    oss.Ptr(name),
			}
			if uploadOpts.ContentType != "" {
				initReq.ContentType = oss.Ptr(uploadOpts.ContentType)
			}
			init, err := b.client.InitiateMultipartUpload(ctx, initReq)
			if err != nil {
				return errors.Wrap(err, "failed to initiate multi-part upload")
			}
			chunk := 0
			uploadEveryPart := func(everypartsize int64, cnk int) (oss.UploadPart, error) {
				req := &oss.UploadPartRequest{
					Bucket:        oss.Ptr(b.name),
					Key:           oss.Ptr(name),
					UploadId:      init.UploadId,
					PartNumber:    int32(cnk),
					ContentLength: oss.Ptr(everypartsize),
					Body:          io.LimitReader(ncloser, everypartsize),
				}
				prt, err := b.client.UploadPart(ctx, req)
				if err != nil {
					_, abortErr := b.client.AbortMultipartUpload(ctx, &oss.AbortMultipartUploadRequest{
						Bucket:   oss.Ptr(b.name),
						Key:      oss.Ptr(name),
						UploadId: init.UploadId,
					})
					if abortErr != nil {
						return oss.UploadPart{}, errors.Wrap(abortErr, "failed to abort multi-part upload after upload part error: "+err.Error())
					}
					return oss.UploadPart{}, errors.Wrap(err, "failed to upload multi-part chunk")
				}
				return oss.UploadPart{
					PartNumber: int32(cnk),
					ETag:       prt.ETag,
				}, nil
			}
			var parts []oss.UploadPart
			for ; chunk < chunksnum; chunk++ {
				part, err := uploadEveryPart(PartSize, chunk+1)
				if err != nil {
					return errors.Wrap(err, "failed to upload every part")
				}
				parts = append(parts, part)
			}
			if lastslice != 0 {
				part, err := uploadEveryPart(lastslice, chunksnum+1)
				if err != nil {
					return errors.Wrap(err, "failed to upload the last chunk")
				}
				parts = append(parts, part)
			}
			_, err = b.client.CompleteMultipartUpload(ctx, &oss.CompleteMultipartUploadRequest{
				Bucket:   oss.Ptr(b.name),
				Key:      oss.Ptr(name),
				UploadId: init.UploadId,
				CompleteMultipartUpload: &oss.CompleteMultipartUpload{
					Parts: parts,
				},
			})
			if err != nil {
				return errors.Wrap(err, "failed to set multi-part upload completive")
			}
		}
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if _, err := b.client.DeleteObject(ctx, &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(b.name),
		Key:    oss.Ptr(name),
	}); err != nil {
		return errors.Wrap(err, "delete oss object")
	}
	return nil
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	resp, err := b.client.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: oss.Ptr(b.name),
		Key:    oss.Ptr(name),
	})
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	var mod time.Time
	if resp.LastModified != nil {
		mod = *resp.LastModified
	}

	return objstore.ObjectAttributes{
		Size:         resp.ContentLength,
		LastModified: mod,
	}, nil
}

// NewBucket returns a new Bucket using the provided oss config values.
func NewBucket(logger log.Logger, conf []byte, component string, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (*Bucket, error) {
	var config Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, errors.Wrap(err, "parse aliyun oss config file failed")
	}
	return NewBucketWithConfig(logger, config, component, wrapRoundtripper)
}

// getCredentialsProvider returns the appropriate credentials provider based on the configuration.
func getCredentialsProvider(config Config) (aliossCred.CredentialsProvider, error) {
	// 1. Use static AK/SK if explicitly provided in the config.
	if config.AccessKeyID != "" && config.AccessKeySecret != "" {
		return aliossCred.NewStaticCredentialsProvider(config.AccessKeyID, config.AccessKeySecret), nil
	}

	var credCfg *alicred.Config
	// 2. Use explicit RRSA (OIDC role) config if role_arn, oidc_provider_arn, and oidc_token_file_path are provided.
	if config.RoleArn != "" && config.OIDCProviderArn != "" && config.OIDCTokenFilePath != "" {
		credCfg = &alicred.Config{
			Type:              oss.Ptr("oidc_role_arn"),
			RoleArn:           oss.Ptr(config.RoleArn),
			OIDCProviderArn:   oss.Ptr(config.OIDCProviderArn),
			OIDCTokenFilePath: oss.Ptr(config.OIDCTokenFilePath),
		}
		if config.RoleSessionName != "" {
			credCfg.RoleSessionName = oss.Ptr(config.RoleSessionName)
		}
	}

	// 3. Fallback to default credential provider chain (e.g., reading from environment variables) if credCfg is nil.
	cred, err := alicred.NewCredential(credCfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create aliyun credential")
	}

	cp := aliossCred.CredentialsProviderFunc(func(ctx context.Context) (aliossCred.Credentials, error) {
		model, err := cred.GetCredential()
		if err != nil {
			return aliossCred.Credentials{}, err
		}
		var accessKeyID, accessKeySecret, securityToken string
		if model.AccessKeyId != nil {
			accessKeyID = *model.AccessKeyId
		}
		if model.AccessKeySecret != nil {
			accessKeySecret = *model.AccessKeySecret
		}
		if model.SecurityToken != nil {
			securityToken = *model.SecurityToken
		}
		return aliossCred.Credentials{
			AccessKeyID:     accessKeyID,
			AccessKeySecret: accessKeySecret,
			SecurityToken:   securityToken,
		}, nil
	})

	return cp, nil
}

// NewBucketWithConfig returns a new Bucket using the provided oss config struct.
func NewBucketWithConfig(logger log.Logger, config Config, component string, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (*Bucket, error) {
	if err := validate(config); err != nil {
		return nil, err
	}

	cfg := oss.LoadDefaultConfig().WithEndpoint(config.Endpoint)

	cp, err := getCredentialsProvider(config)
	if err != nil {
		return nil, err
	}
	cfg.WithCredentialsProvider(cp)

	if wrapRoundtripper != nil {
		rt, err := exthttp.DefaultTransport(exthttp.DefaultHTTPConfig)
		if err != nil {
			return nil, err
		}
		cfg.WithHttpClient(&http.Client{
			Transport: wrapRoundtripper(rt),
		})
	}

	client := oss.NewClient(cfg)

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
		config: config,
	}
	return bkt, nil
}

// validate checks to see the config options are set.
func validate(config Config) error {
	if config.Endpoint == "" || config.Bucket == "" {
		return errors.New("aliyun oss endpoint or bucket is not present in config file")
	}

	return nil
}

func (b *Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive}
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}

	delimiter := objstore.DirDelim
	if objstore.ApplyIterOptions(options...).Recursive {
		delimiter = ""
	}

	var continuationToken *string
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context closed while iterating bucket")
		}

		req := &oss.ListObjectsV2Request{
			Bucket: oss.Ptr(b.name),
			Prefix: oss.Ptr(dir),
		}
		if delimiter != "" {
			req.Delimiter = oss.Ptr(delimiter)
		}
		if continuationToken != nil {
			req.ContinuationToken = continuationToken
		}

		objects, err := b.client.ListObjectsV2(ctx, req)
		if err != nil {
			return errors.Wrap(err, "listing aliyun oss bucket failed")
		}

		for _, object := range objects.Contents {
			if object.Key != nil {
				if err := f(*object.Key); err != nil {
					return errors.Wrapf(err, "callback func invoke for object %s failed ", *object.Key)
				}
			}
		}

		for _, object := range objects.CommonPrefixes {
			if object.Prefix != nil {
				if err := f(*object.Prefix); err != nil {
					return errors.Wrapf(err, "callback func invoke for directory %s failed", *object.Prefix)
				}
			}
		}

		if !objects.IsTruncated {
			break
		}
		continuationToken = objects.NextContinuationToken
	}

	return nil
}

func (b *Bucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	return b.Iter(ctx, dir, func(name string) error {
		return f(objstore.IterObjectAttributes{Name: name})
	}, options...)
}

func (b *Bucket) Name() string {
	return b.name
}

func NewTestBucketFromConfig(t testing.TB, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	if c.Bucket == "" {
		src := rand.NewSource(time.Now().UnixNano())

		bktToCreate := strings.ReplaceAll(fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63()), "_", "-")
		if len(bktToCreate) >= 63 {
			bktToCreate = bktToCreate[:63]
		}

		cfg := oss.LoadDefaultConfig().WithEndpoint(c.Endpoint)
		cp, err := getCredentialsProvider(c)
		if err != nil {
			return nil, nil, errors.Wrap(err, "get credentials provider for test client failed")
		}
		cfg.WithCredentialsProvider(cp)
		testclient := oss.NewClient(cfg)

		if _, err := testclient.PutBucket(context.Background(), &oss.PutBucketRequest{
			Bucket: oss.Ptr(bktToCreate),
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "create aliyun oss bucket %s failed", bktToCreate)
		}
		c.Bucket = bktToCreate
	}

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-aliyun-oss-test", nil)
	if err != nil {
		return nil, nil, err
	}

	if reuseBucket {
		if err := b.Iter(context.Background(), "", func(_ string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "oss check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "Aliyun OSS bucket for OSS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if _, err := b.client.DeleteBucket(context.Background(), &oss.DeleteBucketRequest{
			Bucket: oss.Ptr(c.Bucket),
		}); err != nil {
			t.Logf("deleting bucket %s failed: %s", c.Bucket, err)
		}
	}, nil
}

func (b *Bucket) Close() error { return nil }

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("given object name should not empty")
	}

	req := &oss.GetObjectRequest{
		Bucket: oss.Ptr(b.name),
		Key:    oss.Ptr(name),
	}
	if length != -1 {
		req.Range = oss.Ptr(fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	}

	resp, err := b.client.GetObject(ctx, req)
	if err != nil {
		return nil, err
	}

	return objstore.ObjectSizerReadCloser{
		ReadCloser: resp.Body,
		Size: func() (int64, error) {
			return resp.ContentLength, nil
		},
	}, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	exists, err := b.client.IsObjectExist(ctx, b.name, name)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "cloud not check if object exists")
	}

	return exists, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	var aliErr *oss.ServiceError
	if errors.As(err, &aliErr) {
		if aliErr.StatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *Bucket) IsAccessDeniedErr(err error) bool {
	var aliErr *oss.ServiceError
	if errors.As(err, &aliErr) {
		if aliErr.StatusCode == http.StatusForbidden {
			return true
		}
	}
	return false
}
