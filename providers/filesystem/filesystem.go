// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filesystem

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/efficientgo/core/errcapture"
	"github.com/pkg/errors"
	"github.com/pkg/xattr"
	"gopkg.in/yaml.v2"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/thanos-io/objstore"
)

var errConditionNotMet = errors.New("filesystem: upload condition not met")

const xAttrKey = "user.thanos.objstore.sha256sum"

// Config stores the configuration for storing and accessing blobs in filesystem.
type Config struct {
	Directory string `yaml:"directory"`
}

// Bucket implements the objstore.Bucket interfaces against filesystem that binary runs on.
// Methods from Bucket interface are thread-safe. Objects are assumed to be immutable.
// NOTE: It does not follow symbolic links.
type Bucket struct {
	rootDir string
}

// NewBucketFromConfig returns a new filesystem.Bucket from config.
func NewBucketFromConfig(conf []byte) (*Bucket, error) {
	var c Config
	if err := yaml.Unmarshal(conf, &c); err != nil {
		return nil, err
	}
	if c.Directory == "" {
		return nil, errors.New("missing directory for filesystem bucket")
	}
	return NewBucket(c.Directory)
}

// NewBucket returns a new filesystem.Bucket.
func NewBucket(rootDir string) (*Bucket, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	return &Bucket{rootDir: absDir}, nil
}

func (b *Bucket) Provider() objstore.ObjProvider { return objstore.FILESYSTEM }

func (b *Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

func (b *Bucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	params := objstore.ApplyIterOptions(options...)
	absDir := filepath.Join(b.rootDir, dir)
	info, err := os.Stat(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "stat %s", absDir)
	}
	if !info.IsDir() {
		return nil
	}

	files, err := os.ReadDir(absDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		name := filepath.Join(dir, file.Name())

		if file.IsDir() {
			empty, err := isDirEmpty(filepath.Join(absDir, file.Name()))
			if err != nil {
				return err
			}

			if empty {
				// Skip empty directories.
				continue
			}

			name += objstore.DirDelim

			if params.Recursive {
				// Recursively list files in the subdirectory.
				if err := b.IterWithAttributes(ctx, name, f, options...); err != nil {
					return err
				}

				// The callback f() has already been called for the subdirectory
				// files so we should skip to next filesystem entry.
				continue
			}
		}

		attrs := objstore.IterObjectAttributes{
			Name: name,
		}
		if params.LastModified {
			absPath := filepath.Join(absDir, file.Name())
			stat, err := os.Stat(absPath)
			if err != nil {
				return errors.Wrapf(err, "stat %s", name)
			}
			attrs.SetLastModified(stat.ModTime())
		}
		if err := f(attrs); err != nil {
			return err
		}
	}
	return nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	// Only include recursive option since attributes are not used in this method.
	var filteredOpts []objstore.IterOption
	for _, opt := range opts {
		if opt.Type == objstore.Recursive {
			filteredOpts = append(filteredOpts, opt)
			break
		}
	}

	return b.IterWithAttributes(ctx, dir, func(attrs objstore.IterObjectAttributes) error {
		return f(attrs.Name)
	}, filteredOpts...)
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.GetRange(ctx, name, 0, -1)
}

type rangeReaderCloser struct {
	io.Reader
	f *os.File
}

func (r *rangeReaderCloser) Close() error {
	return r.f.Close()
}

// Attributes returns information about the specified object. Only returns the version metadata if the object was written with a supported version.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if ctx.Err() != nil {
		return objstore.ObjectAttributes{}, ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)
	stat, err := os.Stat(file)
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrapf(err, "stat %s", file)
	}

	if !slices.Contains(b.SupportedObjectUploadOptions(), objstore.IfMatch) && !slices.Contains(b.SupportedObjectUploadOptions(), objstore.IfNotMatch) {
		return objstore.ObjectAttributes{
			Size:         stat.Size(),
			LastModified: stat.ModTime(),
		}, nil
	}

	var version *objstore.ObjectVersion

	if xattr.XATTR_SUPPORTED {
		chkSum, err := b.checksum(name)
		if err != nil {
			return objstore.ObjectAttributes{}, err
		}
		if chkSum != "" {
			version = &objstore.ObjectVersion{
				Type:  objstore.ETag,
				Value: chkSum,
			}
		}
	}

	return objstore.ObjectAttributes{
		Size:         stat.Size(),
		LastModified: stat.ModTime(),
		Version:      version,
	}, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if name == "" {
		return nil, errors.New("object name is empty")
	}

	var (
		file = filepath.Join(b.rootDir, name)
		stat os.FileInfo
		err  error
	)
	if stat, err = os.Stat(file); err != nil {
		return nil, errors.Wrapf(err, "stat %s", file)
	}

	f, err := os.OpenFile(filepath.Clean(file), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	var newOffset int64
	if off > 0 {
		newOffset, err = f.Seek(off, 0)
		if err != nil {
			return nil, errors.Wrapf(err, "seek %v", off)
		}
	}

	size := stat.Size() - newOffset
	if length == -1 {
		return objstore.ObjectSizerReadCloser{
			ReadCloser: f,
			Size: func() (int64, error) {
				return size, nil
			},
		}, nil
	}

	return objstore.ObjectSizerReadCloser{
		ReadCloser: &rangeReaderCloser{
			Reader: io.LimitReader(f, length),
			f:      f,
		},
		Size: func() (int64, error) {
			return min(length, size), nil
		},
	}, nil
}

// Exists checks if the given directory exists in memory.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	info, err := os.Stat(filepath.Join(b.rootDir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "stat %s", filepath.Join(b.rootDir, name))
	}
	return !info.IsDir(), nil
}

func openSwap(name string) (swf *os.File, err error) {
	for {
		swf, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err == nil {
			return
		} else if !errors.Is(err, fs.ErrExist) {
			return
		}
	}
}

func tryOpenFile(name string, ifNotExists bool) (exists bool, err error) {
	// First try to open the file with exclusive create, then truncate if permitted
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	var f *os.File
	f, err = os.OpenFile(name, flags, 0666)
	if errors.Is(err, fs.ErrExist) && !ifNotExists {
		exists = true
		flags = os.O_RDWR | os.O_CREATE | os.O_APPEND
		f, err = os.OpenFile(name, flags, 0666)
	}
	if err != nil && f != nil {
		err = f.Close()
	}
	return
}

// Upload writes the file specified in src to into the memory.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) (err error) {

	if err := objstore.ValidateUploadOptions(b.SupportedObjectUploadOptions(), opts...); err != nil {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)
	swap := filepath.Join(b.rootDir, fmt.Sprintf("%s.swap", name))
	if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
		return err
	}

	params := objstore.ApplyObjectUploadOptions(opts...)

	// Filesystem provider for debugging & troubleshooting uses a swap file as a file lock.
	swf, err := openSwap(swap)
	if err != nil {
		return err
	}
	clearSwap := func() error {
		err := os.Remove(swap)
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}
	defer errcapture.Do(&err, swf.Close, "close")
	defer errcapture.Do(&err, clearSwap, "remove swap")

	exists, err := tryOpenFile(file, params.IfNotExists)
	if err != nil {
		return err
	}

	if err := b.checkConditions(name, params, exists); err != nil {
		return err
	}

	if xattr.XATTR_SUPPORTED {
		h := sha256.New()
		writer := io.MultiWriter(swf, h)
		if _, err := io.Copy(writer, r); err != nil {
			return errors.Wrapf(err, "copy to %s", swap)
		}
		// Write the checksum into an xattr
		if err := xattr.Set(swap, xAttrKey, h.Sum(nil)); err != nil {
			return err
		}
	}

	// Move swap into target, atomic on unix for which IfNotExists is supported. Will be same mount as is same directory.
	if err := os.Rename(swap, file); err != nil {
		return err
	}

	return
}

// checksum reads an X-Attr for the checksum property if it was written with the file, or empty string if it was not.
// Does not check if X-Attrs are supported on the host - this must be done by the caller.
func (b *Bucket) checksum(name string) (string, error) {
	file := filepath.Join(b.rootDir, name)
	bytes, err := xattr.Get(file, xAttrKey)
	if err != nil {
		return "", err // Legacy filesystem buckets would just return empty string for the version (until objects updated)
	}
	return string(bytes), nil
}

func (b *Bucket) checkConditions(name string, params objstore.UploadObjectParams, exists bool) error {
	if params.Condition != nil && !exists && !params.IfNotMatch {
		return errConditionNotMet
	}
	if params.Condition != nil && exists {
		if params.Condition.Type != objstore.ETag {
			return errConditionNotMet
		}
		chkSum, err := b.checksum(name)
		if err != nil {
			return err
		}
		if params.IfNotMatch && chkSum == params.Condition.Value {
			return errConditionNotMet
		} else if !params.IfNotMatch && chkSum != params.Condition.Value {
			return errConditionNotMet
		}
	}
	//... if the file doesn't exist, and it's an IfNotMatch, that's always fine
	return nil
}

func (b *Bucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	if xattr.XATTR_SUPPORTED {
		return []objstore.ObjectUploadOptionType{objstore.IfNotExists, objstore.IfMatch, objstore.IfNotMatch}
	}
	return []objstore.ObjectUploadOptionType{}
}

func isDirEmpty(name string) (ok bool, err error) {
	f, err := os.Open(filepath.Clean(name))
	if os.IsNotExist(err) {
		// The directory doesn't exist. We don't consider it an error and we treat it like empty.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer errcapture.Do(&err, f.Close, "close dir")

	if _, err = f.Readdir(1); err == io.EOF || os.IsNotExist(err) {
		return true, nil
	}
	return false, err
}

// Delete removes all data prefixed with the dir.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)
	for file != b.rootDir {
		if err := os.RemoveAll(file); err != nil {
			return errors.Wrapf(err, "rm %s", file)
		}
		file = filepath.Dir(file)
		empty, err := isDirEmpty(file)
		if err != nil {
			return err
		}
		if !empty {
			break
		}
	}
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return os.IsNotExist(errors.Cause(err))
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *Bucket) IsAccessDeniedErr(_ error) bool {
	return false
}

// IsConditionNotMetErr returns true if the error is an internal condition not met error or a ErrExist filesystem error
func (b *Bucket) IsConditionNotMetErr(err error) bool {
	return errors.Is(err, errConditionNotMet) || errors.Is(err, fs.ErrExist)
}

func (b *Bucket) Close() error { return nil }

// Name returns the bucket name.
func (b *Bucket) Name() string {
	return fmt.Sprintf("fs: %s", b.rootDir)
}
