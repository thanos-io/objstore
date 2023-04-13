// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storj

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"gopkg.in/yaml.v2"
	"storj.io/common/fpath"
	"storj.io/uplink"
)

// Config stores the configuration for the storj bucket.
type Config struct {
	AccessGrant string `yaml:"access"`
	Bucket      string `yaml:"bucket"`
}

// Bucket implements the store.Bucket interface against storj API.
type Bucket struct {
	logger  log.Logger
	project *uplink.Project
	bucket  *uplink.Bucket
}

// NewBucket returns a new Bucket using the provided storj config values.
func NewBucket(logger log.Logger, config []byte, component string) (*Bucket, error) {
	var conf Config

	err := yaml.Unmarshal(config, &conf)
	if err != nil {
		return nil, err
	}

	return NewBucketWithConfig(logger, conf, component)
}

// NewBucketWithConfig returns a new Bucket using the provided storj config struct.
func NewBucketWithConfig(logger log.Logger, config Config, component string) (*Bucket, error) {
	var instance Bucket

	ctx := fpath.WithTempData(context.TODO(), "", true)

	uplConf := &uplink.Config{
		UserAgent: fmt.Sprintf("thanos-%s", component),
	}

	parsedAccess, err := uplink.ParseAccess(config.AccessGrant)
	if err != nil {
		return nil, err
	}

	instance.project, err = uplConf.OpenProject(ctx, parsedAccess)
	if err != nil {
		return nil, err
	}

	instance.bucket, err = instance.project.EnsureBucket(ctx, config.Bucket)
	if err != nil {
		//Ignoring the error to return the one that occurred first, while still trying to clean up.
		_ = instance.project.Close()
		return nil, err
	}

	instance.logger = logger

	return &instance, nil
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return b.bucket.Name
}

// Close closes the Bucket.
func (b *Bucket) Close() error {
	return b.project.Close()
}

// Iter calls f for each entry in the given directory (not recursive). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {

	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}

	opt := uplink.ListObjectsOptions{
		Recursive: objstore.ApplyIterOptions(options...).Recursive,
		Prefix:    dir,
	}

	iter := b.project.ListObjects(ctx, b.bucket.Name, &opt)

	for iter.Next() {
		if err := f(iter.Item().Key); err != nil {
			return err
		}
	}
	return iter.Err()
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	_ = level.Debug(b.logger).Log("Getting attributes %s from Storj Bucket", name)

	options := uplink.DownloadOptions{}

	download, err := b.project.DownloadObject(fpath.WithTempData(ctx, "", true), b.bucket.Name, name, &options)
	if err != nil {
		return nil, err
	}

	return download, nil
}

// GetRange returns a reader to the range for the given object name.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	_ = level.Debug(b.logger).Log("Getting range of %s from Storj Bucket", name)

	options := uplink.DownloadOptions{
		Offset: off,
		Length: length,
	}

	download, err := b.project.DownloadObject(fpath.WithTempData(ctx, "", true), b.bucket.Name, name, &options)
	if err != nil {
		return nil, err
	}

	return download, nil
}

// Exists returns whether the object with the given name exists or not.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_ = level.Debug(b.logger).Log("Ensuring %s exists in Storj Bucket", name)
	_, err := b.project.StatObject(fpath.WithTempData(ctx, "", true), b.bucket.Name, name)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
	}
	return true, err
}

// IsObjNotFoundErr returns whether the given error matches the object stores non found error.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return errors.Is(err, uplink.ErrObjectNotFound)
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	_ = level.Debug(b.logger).Log("Getting attributes %s from Storj Bucket", name)

	attr := objstore.ObjectAttributes{}

	obj, err := b.project.StatObject(fpath.WithTempData(ctx, "", true), b.bucket.Name, name)
	if err != nil {
		return attr, err
	}

	attr.Size = obj.System.ContentLength
	attr.LastModified = obj.System.Created

	return attr, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	var uploadOptions *uplink.UploadOptions

	_ = level.Debug(b.logger).Log("Uploading %s to Storj Bucket", name)

	writer, err := b.project.UploadObject(fpath.WithTempData(ctx, "", true), b.bucket.Name, name, uploadOptions)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, r)
	if err != nil {
		//Ignoring the error to return the one that occurred first, while still trying to clean up.
		_ = writer.Abort()
		return err
	}

	return writer.Commit()
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	var err error

	_ = level.Debug(b.logger).Log("Deleting %s from Storj Bucket", name)

	_, err = b.project.DeleteObject(fpath.WithTempData(ctx, "", true), b.bucket.Name, name)

	return err
}
