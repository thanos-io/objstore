package azure

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	azdatalakeerror "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	azfile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	azfilesystem "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"gopkg.in/yaml.v2"
)

type DataLakeGen2Bucket struct {
	logger           log.Logger
	containerName    string
	readerMaxRetries int
	filesystemClient *azfilesystem.Client
}

func NewDataLakeGen2Bucket(logger log.Logger, conf Config, component string, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (*DataLakeGen2Bucket, error) {
	filesystemClient, err := getDataLakeGen2FilesystemClient(conf, wrapRoundtripper)
	if err != nil {
		return nil, err
	}

	if conf.StorageCreateContainer {
		ctx := context.Background()
		_, err = filesystemClient.GetProperties(ctx, nil)
		if err != nil {
			if !azdatalakeerror.HasCode(err, azdatalakeerror.FileSystemNotFound) {
				return nil, err
			}

			_, err := filesystemClient.Create(ctx, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "error creating Azure Data Lake Gen2 filesystem: %s", conf.ContainerName)
			}

			level.Info(logger).Log("msg", "Azure Data Lake Gen2 filesystem successfully created", "address", conf.ContainerName)
		}
	}

	bkt := &DataLakeGen2Bucket{
		logger:           logger,
		containerName:    conf.ContainerName,
		readerMaxRetries: conf.ReaderConfig.MaxRetryRequests,
		filesystemClient: filesystemClient,
	}

	return bkt, nil
}

func (b *DataLakeGen2Bucket) Provider() objstore.ObjProvider { return objstore.AZURE }

func (b *DataLakeGen2Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

func (b *DataLakeGen2Bucket) IterWithAttributes(ctx context.Context,
	dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	prefix := dir
	if prefix != "" && !strings.HasSuffix(prefix, DirDelim) {
		prefix += DirDelim
	}

	params := objstore.ApplyIterOptions(options...)
	pager := b.filesystemClient.NewListPathsPager(params.Recursive, &azfilesystem.ListPathsOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			// azdatalake will complain if the prefix path doesn't
			// exist, we need to handle that properly.
			if b.IsObjNotFoundErr(err) {
				return nil
			} else {
				return err
			}
		}

		for _, path := range page.Paths {
			if path.Name == nil {
				continue
			}

			name := *path.Name

			if path.IsDirectory != nil && *path.IsDirectory {
				if params.Recursive {
					continue
				}

				if !strings.HasSuffix(name, DirDelim) {
					name += DirDelim
				}
			}

			attrs := objstore.IterObjectAttributes{
				Name: name,
			}

			if params.LastModified && path.LastModified != nil {
				parsedTime, err := time.Parse(time.RFC1123, *path.LastModified)
				if err != nil {
					return errors.Wrapf(err, "cannot parse last modified time for blob: %s", *path.Name)
				}
				attrs.SetLastModified(parsedTime)
			}

			if err := f(attrs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *DataLakeGen2Bucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
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

func (b *DataLakeGen2Bucket) IsObjNotFoundErr(err error) bool {
	if err == nil {
		return false
	}

	return azdatalakeerror.HasCode(err,
		azdatalakeerror.PathNotFound, azdatalakeerror.InvalidURI)
}

func (b *DataLakeGen2Bucket) IsAccessDeniedErr(err error) bool {
	if err == nil {
		return false
	}

	return azdatalakeerror.HasCode(err,
		azdatalakeerror.AuthorizationPermissionMismatch, azdatalakeerror.InsufficientAccountPermissions)
}

func (b *DataLakeGen2Bucket) getBlobReader(ctx context.Context, name string, httpRange azfile.HTTPRange) (io.ReadCloser, error) {
	level.Debug(b.logger).Log("msg", "getting blob", "blob", name, "offset", httpRange.Offset, "length", httpRange.Count)
	if name == "" {
		return nil, errors.New("blob name cannot be empty")
	}

	fileClient := b.filesystemClient.NewFileClient(name)
	resp, err := fileClient.DownloadStream(ctx, &azfile.DownloadStreamOptions{Range: &httpRange})
	if err != nil {
		return nil, err
	}

	return objstore.ObjectSizerReadCloser{
		ReadCloser: resp.NewRetryReader(ctx, &azfile.RetryReaderOptions{MaxRetries: int32(b.readerMaxRetries)}),
		Size: func() (int64, error) {
			return *resp.ContentLength, nil
		},
	}, nil
}

func (b *DataLakeGen2Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	// for azure data lake gen 2, the folder name doesn't have '/' in the suffix.
	name = strings.TrimSuffix(name, DirDelim)
	return b.getBlobReader(ctx, name, azfile.HTTPRange{})
}

func (b *DataLakeGen2Bucket) GetRange(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	// for azure data lake gen 2, the folder name doesn't have '/' in the suffix.
	name = strings.TrimSuffix(name, DirDelim)
	return b.getBlobReader(ctx, name, azfile.HTTPRange{Offset: offset, Count: length})
}

func (b *DataLakeGen2Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	// for azure data lake gen 2, the folder name doesn't have '/' in the suffix.
	name = strings.TrimSuffix(name, DirDelim)

	level.Debug(b.logger).Log("msg", "Getting blob attributes", "blob", name)
	fileClient := b.filesystemClient.NewFileClient(name)

	props, err := fileClient.GetProperties(ctx, nil)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	var (
		contentLength int64
		lastModified  time.Time
	)

	if props.ContentLength != nil {
		contentLength = *props.ContentLength
	}

	if props.LastModified != nil {
		lastModified = *props.LastModified
	}

	return objstore.ObjectAttributes{
		Size:         contentLength,
		LastModified: lastModified,
	}, nil
}

func (b *DataLakeGen2Bucket) Exists(ctx context.Context, name string) (bool, error) {
	// for azure data lake gen 2, the folder name doesn't have '/' in the suffix.
	name = strings.TrimSuffix(name, DirDelim)

	level.Debug(b.logger).Log("msg", "checking if blob exists", "blob", name)
	fileClient := b.filesystemClient.NewFileClient(name)

	_, err := fileClient.GetProperties(ctx, nil)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "cannot get properties for Azure blob, address: %s", name)
	}

	return true, nil
}

func (b *DataLakeGen2Bucket) Upload(ctx context.Context, name string, r io.Reader, uploadOpts ...objstore.ObjectUploadOption) error {
	// for azure data lake gen 2, the folder name doesn't have '/' in the suffix.
	name = strings.TrimSuffix(name, DirDelim)

	level.Debug(b.logger).Log("msg", "uploading blob", "blob", name)
	fileClient := b.filesystemClient.NewFileClient(name)

	exists, err := b.Exists(ctx, name)
	if err != nil {
		return errors.Wrapf(err, "cannot check existence of blob before upload, address: %s", name)
	}

	if !exists {
		// unlike azblob sdk, azdatalake in the Go SDK
		// requires you to explicitly create the file before you can upload data to it.
		_, err := fileClient.Create(ctx, nil)
		if err != nil {
			return errors.Wrapf(err, "cannot create blob before upload, address: %s", name)
		}
	}

	uploadOptions := objstore.ApplyObjectUploadOptions(uploadOpts...)
	opts := &azfile.UploadStreamOptions{
		ChunkSize:   3 * 1024 * 1024,
		Concurrency: 4,
		HTTPHeaders: &azfile.HTTPHeaders{
			ContentType: &uploadOptions.ContentType,
		},
	}

	if err := fileClient.UploadStream(ctx, r, opts); err != nil {
		return errors.Wrapf(err, "cannot upload blob, address: %s", name)
	}

	return nil
}

func (b *DataLakeGen2Bucket) Delete(ctx context.Context, name string) error {
	// for azure data lake gen 2, the folder name doesn't have '/' in the suffix.
	name = strings.TrimSuffix(name, DirDelim)

	level.Debug(b.logger).Log("msg", "deleting blob", "blob", name)
	fileClient := b.filesystemClient.NewFileClient(name)

	_, err := fileClient.Delete(ctx, nil)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return nil
		}
		return errors.Wrapf(err, "cannot delete blob, address: %s", name)
	}

	return nil
}

func (b *DataLakeGen2Bucket) Name() string {
	return b.containerName
}

func (b *DataLakeGen2Bucket) Close() error {
	return nil
}

// NewTestDataLakeGen2Bucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestDataLakeGen2Bucket(t testing.TB, component string) (objstore.Bucket, func(), error) {
	t.Log("Using test Azure data lake gen 2 bucket.")

	conf := &DefaultConfig
	conf.StorageAccountName = os.Getenv("AZURE_STORAGE_ACCOUNT")
	conf.StorageAccountKey = os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	conf.ContainerName = objstore.CreateTemporaryTestBucketName(t)

	bc, err := yaml.Marshal(conf)
	if err != nil {
		return nil, nil, err
	}
	bkt, err := NewBucket(log.NewNopLogger(), bc, component, nil)
	if err != nil {
		t.Errorf("Cannot create Azure storage container:")
		return nil, nil, err
	}
	ctx := context.Background()
	return bkt, func() {
		objstore.EmptyBucket(t, ctx, bkt)
		_, err := bkt.(*DataLakeGen2Bucket).filesystemClient.Delete(ctx, nil)
		if err != nil {
			t.Logf("deleting bucket failed: %s", err)
		}
	}, nil
}
