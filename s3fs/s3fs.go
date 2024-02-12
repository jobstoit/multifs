package s3fs

import (
	"bytes"
	"context"
	"io/fs"
	"path"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jobstoit/multifs"
)

// FS is an s3 implementation of multifs.FS
type FS struct {
	bucketname string
	cli        *s3.Client

	downloader *manager.Downloader
	uploader   *manager.Uploader
}

// New returns a new s3 implementation for multifs.FS
func New(bucketname string, cli *s3.Client) *FS {
	return &FS{
		bucketname: bucketname,
		cli:        cli,
		downloader: manager.NewDownloader(cli),
		uploader:   manager.NewUploader(cli),
	}
}

// Open is an implementation of multifs.FS
func (f *FS) Open(name string) (multifs.File, error) {
	return f.OpenFileContext(context.Background(), name, multifs.O_RDONLY, 0)
}

// OpenContext is an implenentation of multifs.FS
func (f *FS) OpenContext(ctx context.Context, name string) (multifs.File, error) {
	return f.OpenFileContext(ctx, name, multifs.O_RDONLY, 0)
}

// OpenFile is an implenetation of multifs.FS
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (multifs.File, error) {
	return f.OpenFileContext(context.Background(), name, flag, perm)
}

// OpenFileContext is an implementation of multifs.FS
func (f *FS) OpenFileContext(ctx context.Context, name string, flag int, perm fs.FileMode) (multifs.File, error) {
	_, err := f.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &f.bucketname,
		Key:    &name,
	})
	if err != nil {
		if flag&multifs.O_CREATE == 0 {
			return nil, fs.ErrNotExist
		}

		if err := f.createFile(ctx, name); err != nil {
			return nil, fs.ErrInvalid
		}
	}

	return &File{
		fs:   f,
		key:  name,
		flag: flag,
		buf:  multifs.NewByteBuffer([]byte{}),
	}, nil
}

func (f *FS) createFile(ctx context.Context, name string) error {
	_, err := f.cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &f.bucketname,
		Key:    &name,
		Body:   bytes.NewBufferString(""),
	})

	return err
}

// Mkdir is an implementaion of multifs.FS
func (f *FS) Mkdir(name string, perm fs.FileMode) error {
	return f.MkdirContext(context.Background(), name, perm)
}

// MkdirContext is an implementaion of multifs.FS
func (f *FS) MkdirContext(ctx context.Context, name string, perm fs.FileMode) error {
	return multifs.ErrNotImplemented
}

// MkdirAll is an implementaion of multifs.FS
func (f *FS) MkdirAll(name string, perm fs.FileMode) error {
	return f.MkdirAllContext(context.Background(), name, perm)
}

// MkdirAllContext is an implementaion of multifs.FS
func (f *FS) MkdirAllContext(ctx context.Context, name string, perm fs.FileMode) error {
	return multifs.ErrNotImplemented
}

// Remove is an implementation of multifs.FS
func (f *FS) Remove(name ...string) error {
	return f.RemoveContext(context.Background(), name...)
}

// RemoveContext is an implementation of multifs.FS
func (f *FS) RemoveContext(ctx context.Context, name ...string) error {
	toDelete := &types.Delete{}

	for _, n := range name {
		key := n
		toDelete.Objects = append(toDelete.Objects, types.ObjectIdentifier{
			Key: &key,
		})
	}

	_, err := f.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &f.bucketname,
		Delete: toDelete,
	})

	return err
}

// File is an s3 implementation for multifs.File
type File struct {
	fs      *FS
	key     string
	flag    int
	fetched bool
	content []byte
	buf     *multifs.ByteBuffer
}

// Read is an implenentation of multifs.File
func (f *File) Read(p []byte) (int, error) {
	return f.ReadContext(context.Background(), p)
}

// ReadContext is an implenentation of multifs.File
func (f *File) ReadContext(ctx context.Context, p []byte) (int, error) {
	if f.fs == nil {
		return 0, fs.ErrClosed
	}

	if f.flag&multifs.O_RDWR+f.flag&multifs.O_RDONLY == 0 {
		return 0, fs.ErrPermission
	}

	if !f.fetched {
		if err := f.fetchContent(ctx); err != nil {
			return 0, err
		}
	}

	return f.buf.Read(p)
}

func (f *File) fetchContent(ctx context.Context) error {
	if f.fs == nil {
		return fs.ErrClosed
	}

	_, err := f.fs.downloader.Download(ctx, f.buf, &s3.GetObjectInput{
		Bucket: &f.fs.bucketname,
		Key:    &f.key,
	})

	f.fetched = true

	return err
}

// Close is an implementation of multifs.File
func (f *File) Close() error {
	f.fs = nil
	f.buf.Reset()
	f.buf = nil
	return nil
}

// Stat is an implenentation of multifs.File
func (f *File) Stat() (fs.FileInfo, error) {
	return f.StatContext(context.Background())
}

// StatContext is an implenentation of multifs.File
func (f *File) StatContext(ctx context.Context) (fs.FileInfo, error) {
	if f.fs == nil {
		return nil, fs.ErrClosed
	}

	res, err := f.fs.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &f.fs.bucketname,
		Key:    &f.key,
	})
	if err != nil {
		return nil, err
	}

	_, filename := path.Split(f.key)

	return multifs.NewFileInfo(
		filename,
		res.ContentLength,
		0777,
		*res.LastModified,
		false,
		f.fs,
	), nil
}

// Write is an implementation of io.Writer
func (f *File) Write(p []byte) (int, error) {
	return f.WriteContext(context.Background(), p)
}

// WriteContext is extending io.Writer
func (f *File) WriteContext(ctx context.Context, b []byte) (int, error) {
	if f.fs == nil {
		return 0, fs.ErrClosed
	}

	if f.flag&multifs.O_RDWR+f.flag&multifs.O_WRONLY == 0 {
		return 0, fs.ErrPermission
	}

	if f.flag&multifs.O_APPEND > 0 && !f.fetched {
		if err := f.fetchContent(ctx); err != nil {
			return 0, err
		}
	}

	prev := f.buf.Clone()
	amount, err := f.buf.Write(b)
	if err != nil {
		return 0, err
	}

	_, err = f.fs.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &f.fs.bucketname,
		Key:    &f.key,
		Body:   f.buf,
	})
	if err != nil {
		f.buf = prev
		return 0, err
	}

	return amount, nil
}
