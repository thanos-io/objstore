// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// Bucket implements the store.Bucket interface against Azure APIs.
type Bucket struct {
	logger           log.Logger
	containerClient  *azblob.ContainerClient
	containerName    string
	maxRetryRequests int
}

// NewBucket returns a new Bucket using the provided Azure config.
func NewBucket(logger log.Logger, azureConfig []byte, component string) (*Bucket, error) {
	level.Debug(logger).Log("msg", "creating new Azure bucket connection", "component", component)

	// Parse configuration
	conf, err := parseConfig(azureConfig)
	if err != nil {
		return nil, err
	}
	if err := conf.validate(); err != nil {
		return nil, err
	}

	// Create the container client
	containerClient, err := getContainerClient(conf)
	if err != nil {
		return nil, err
	}

	// Check if container exists and create if it does not
	ctx := context.Background()
	_, err = containerClient.GetProperties(ctx, &azblob.ContainerGetPropertiesOptions{})
	if err != nil {
		internalErr, ok := err.(*azblob.InternalError)
		if !ok {
			return nil, errors.Wrapf(err, "Azure API return unexpected error: %T\n", err)
		}
		storageErr := &azblob.StorageError{}
		if ok := internalErr.As(&storageErr); !ok {
			return nil, errors.Wrapf(err, "Azure API return unexpected error: %T\n", err)
		}
		if storageErr.ErrorCode != azblob.StorageErrorCodeContainerNotFound {
			return nil, err
		}
		_, err := containerClient.Create(ctx, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating Azure blob container: %s", conf.ContainerName)
		}
		level.Info(logger).Log("msg", "Azure blob container successfully created", "address", conf.ContainerName)
	}

	bkt := &Bucket{
		logger:           logger,
		containerClient:  containerClient,
		containerName:    conf.ContainerName,
		maxRetryRequests: conf.ReaderConfig.MaxRetryRequests,
	}
	return bkt, nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	prefix := dir
	if prefix != "" && !strings.HasSuffix(prefix, DirDelim) {
		prefix += DirDelim
	}
	params := objstore.ApplyIterOptions(options...)

	if params.Recursive {
		opt := &azblob.ContainerListBlobsFlatOptions{Prefix: &prefix}
		pager := b.containerClient.ListBlobsFlat(opt)
		for pager.NextPage(ctx) {
			resp := pager.PageResponse()
			for _, blob := range resp.Segment.BlobItems {
				if err := f(*blob.Name); err != nil {
					return err
				}
			}
		}
		if err := pager.Err(); err != nil {
			return err
		}
		return nil
	}

	opt := &azblob.ContainerListBlobsHierarchyOptions{Prefix: &prefix}
	pager := b.containerClient.ListBlobsHierarchy(DirDelim, opt)
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, blob := range resp.Segment.BlobItems {
			if err := f(*blob.Name); err != nil {
				return err
			}
		}
		for _, blobPrefix := range resp.Segment.BlobPrefixes {
			if err := f(*blobPrefix.Name); err != nil {
				return err
			}
		}
	}
	if err := pager.Err(); err != nil {
		return err
	}

	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	internalErr, ok := err.(*azblob.InternalError)
	if !ok {
		return false
	}
	storageErr := &azblob.StorageError{}
	if ok := internalErr.As(&storageErr); !ok {
		return false
	}
	return storageErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound || storageErr.ErrorCode == azblob.StorageErrorCodeInvalidURI
}

func (b *Bucket) getBlobReader(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	level.Debug(b.logger).Log("msg", "getting blob", "blob", name, "offset", offset, "length", length)
	if name == "" {
		return nil, errors.New("blob name cannot be empty")
	}
	attr, err := b.Attributes(ctx, name)
	if err != nil {
		return nil, err
	}

	var size int64
	// If a length is specified and it won't go past the end of the file,
	// then set it as the size.
	if length > 0 && length <= attr.Size-offset {
		size = length
		level.Debug(b.logger).Log("msg", "set size to length", "size", size, "length", length, "offset", offset, "name", name)
	} else {
		size = attr.Size - offset
		level.Debug(b.logger).Log("msg", "set size to go to EOF", "contentlength", attr.Size, "size", size, "length", length, "offset", offset, "name", name)
	}

	blobClient, err := b.containerClient.NewBlobClient(name)
	if err != nil {
		return nil, err
	}
	opt := azblob.DownloadOptions{
		BlockSize:   azblob.BlobDefaultDownloadBlockSize,
		Parallelism: uint16(3),
		Progress:    nil,
		RetryReaderOptionsPerBlock: azblob.RetryReaderOptions{
			MaxRetryRequests: b.maxRetryRequests,
		},
	}
	destBuffer := make([]byte, size)
	if err := blobClient.DownloadToBuffer(ctx, offset, size, destBuffer, opt); err != nil {
		return nil, errors.Wrapf(err, "cannot download blob, address: %s", blobClient.URL())
	}
	return ioutil.NopCloser(bytes.NewReader(destBuffer)), nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getBlobReader(ctx, name, 0, azblob.CountToEnd)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getBlobReader(ctx, name, off, length)
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	level.Debug(b.logger).Log("msg", "Getting blob attributes", "blob", name)
	blobClient, err := b.containerClient.NewBlobClient(name)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}
	resp, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return objstore.ObjectAttributes{
		Size:         *resp.ContentLength,
		LastModified: *resp.LastModified,
	}, nil
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	level.Debug(b.logger).Log("msg", "checking if blob exists", "blob", name)
	blobClient, err := b.containerClient.NewBlobClient(name)
	if err != nil {
		return false, err
	}
	if _, err := blobClient.GetProperties(ctx, nil); err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "cannot get properties for Azure blob, address: %s", name)
	}
	return true, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	level.Debug(b.logger).Log("msg", "uploading blob", "blob", name)
	blobClient, err := b.containerClient.NewBlockBlobClient(name)
	if err != nil {
		return err
	}
	opt := azblob.UploadStreamOptions{
		BufferSize: 3 * 1024 * 1024,
		MaxBuffers: 4,
	}
	if _, err := blobClient.UploadStream(ctx, r, opt); err != nil {
		return errors.Wrapf(err, "cannot upload Azure blob, address: %s", name)
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	level.Debug(b.logger).Log("msg", "deleting blob", "blob", name)
	blobClient, err := b.containerClient.NewBlobClient(name)
	if err != nil {
		return err
	}
	opt := &azblob.BlobDeleteOptions{
		DeleteSnapshots: azblob.DeleteSnapshotsOptionTypeInclude.ToPtr(),
	}
	if _, err := blobClient.Delete(ctx, opt); err != nil {
		return errors.Wrapf(err, "error deleting blob, address: %s", name)
	}
	return nil
}

// Name returns Azure container name.
func (b *Bucket) Name() string {
	return b.containerName
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, component string) (objstore.Bucket, func(), error) {
	t.Log("Using test Azure bucket.")

	conf := &Config{
		StorageAccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
		StorageAccountKey:  os.Getenv("AZURE_STORAGE_ACCESS_KEY"),
		ContainerName:      objstore.CreateTemporaryTestBucketName(t),
	}

	bc, err := yaml.Marshal(conf)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()

	bkt, err := NewBucket(log.NewNopLogger(), bc, component)
	if err != nil {
		t.Errorf("Cannot create Azure storage container:")
		return nil, nil, err
	}

	return bkt, func() {
		objstore.EmptyBucket(t, ctx, bkt)
		_, err := bkt.containerClient.Delete(ctx, &azblob.ContainerDeleteOptions{})
		if err != nil {
			t.Logf("deleting bucket failed: %s", err)
		}
	}, nil
}

// Close bucket.
func (b *Bucket) Close() error {
	return nil
}
