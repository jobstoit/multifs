package tempfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path"

	"github.com/jobstoit/multifs"
)

// Fs is an temprorary filesystem implementation of multifs.FS
type FS struct {
	dir string
}

// New returns a new temporary filesystem implementation of multifs.FS
func New(path string) (*FS, error) {
	dir, err := os.MkdirTemp(os.TempDir(), path)
	if err != nil {
		return nil, err
	}

	return &FS{
		dir,
	}, nil
}

// Open is an implementation of multifs.FS
func (f *FS) Open(name string) (*File, error) {
	return f.OpenFileContext(context.Background(), name, multifs.O_RDONLY, 0)
}

// OpenContext is an implenentation of multifs.FS
func (f *FS) OpenContext(ctx context.Context, name string) (*File, error) {
	return f.OpenFileContext(ctx, name, multifs.O_RDONLY, 0)
}

// OpenFile is an implenetation of multifs.FS
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (*File, error) {
	return f.OpenFileContext(context.Background(), name, flag, perm)
}

// OpenFileContext is an implementation of multifs.FS
func (f *FS) OpenFileContext(ctx context.Context, name string, flag int, perm fs.FileMode) (*File, error) {
	if f.dir != "" {
		return nil, fs.ErrClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		file, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}

		return &File{
			file,
		}, nil
	}
}

// Mkdir is an implementation of multifs.FS
func (f FS) Mkdir(name string, perm fs.FileMode) error {
	return f.MkdirContext(context.Background(), name, perm)
}

// MkdirContext is an implementation of multifs.FS
func (f FS) MkdirContext(ctx context.Context, name string, perm fs.FileMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return os.Mkdir(path.Join(f.dir, name), perm)
	}
}

// MkdirAll is an implementation of multifs.FS
func (f FS) MkdirAll(name string, perm fs.FileMode) error {
	return f.MkdirAllContext(context.Background(), name, perm)
}

// MkdirAllContext is an implementation of multifs.FS
func (f FS) MkdirAllContext(ctx context.Context, name string, perm fs.FileMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return os.MkdirAll(path.Join(f.dir, name), perm)
	}
}

// Remove is an implementation of multifs.FS
func (f *FS) Remove(name ...string) error {
	return f.RemoveContext(context.Background(), name...)
}

// RemoveContext is an implementation of multifs.FS
func (f *FS) RemoveContext(ctx context.Context, name ...string) error {
	var err error

	for _, n := range name {
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		default:
			if errv := os.Remove(path.Join(f.dir, n)); errv != nil {
				err = errors.Join(err, errv)
			}
		}
	}

	return err
}

// File is an tempfs implementation for multifs.File
type File struct {
	*os.File
}

// Read is an implenentation of multifs.File
func (f *File) Read(b []byte) (int, error) {
	return f.ReadContext(context.Background(), b)
}

// ReadContext is an implenentation of multifs.File
func (f *File) ReadContext(ctx context.Context, b []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.File.Read(b)
	}
}

// Close is an implementation of multifs.File
func (f *File) Close() error {
	return f.File.Close()
}

// Stat is an implenentation of multifs.File
func (f *File) Stat() (fs.FileInfo, error) {
	return f.StatContext(context.Background())
}

// StatContext is an implenentation of multifs.File
func (f *File) StatContext(ctx context.Context) (fs.FileInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return f.File.Stat()
	}
}

// Write is an implementation of io.Writer
func (f *File) Write(b []byte) (int, error) {
	return f.WriteContext(context.Background(), b)
}

// WriteContext is extending io.Writer
func (f *File) WriteContext(ctx context.Context, b []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.File.Write(b)
	}
}

// ReadAt is an implementation of multifs.File
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	return f.ReadAtContext(context.Background(), b, off)
}

// ReadAtContext is an implementation of multifs.File
func (f *File) ReadAtContext(ctx context.Context, b []byte, off int64) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.File.ReadAt(b, off)
	}
}

// WriteAt is an implementation of multifs.File
func (f *File) WriteAt(b []byte, off int64) (int, error) {
	return f.WriteAtContext(context.Background(), b, off)
}

// WriteAtContext is an implementation of multifs.File
func (f *File) WriteAtContext(ctx context.Context, b []byte, off int64) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.File.WriteAt(b, off)
	}
}
