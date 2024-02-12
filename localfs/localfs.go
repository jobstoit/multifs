package localfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path"

	"github.com/jobstoit/multifs"
)

// FS implments the multifs.FS
type FS struct {
	dir string
}

// New returns a new local FS
func New(path string) multifs.FS {
	return &FS{
		dir: path,
	}
}

// Open is an implentation of multifs.FS
func (f FS) Open(name string) (multifs.File, error) {
	return f.OpenFileContext(context.Background(), name, multifs.O_RDONLY, 0)
}

// OpenContext is an implementation of multifs.FS
func (f FS) OpenContext(ctx context.Context, name string) (multifs.File, error) {
	return f.OpenFileContext(ctx, name, multifs.O_RDONLY, 0)
}

// OpenFile is an implentation of multifs.FS
func (f FS) OpenFile(name string, flag int, perm fs.FileMode) (multifs.File, error) {
	return f.OpenFileContext(context.Background(), name, flag, perm)
}

// OpenFileContext is an implenentation of multifs.FS
func (f FS) OpenFileContext(ctx context.Context, name string, flag int, perm fs.FileMode) (multifs.File, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		fi, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}

		return &File{
			File: fi,
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

// Remove is an implentation of multifs.FS
func (f FS) Remove(name ...string) error {
	return f.RemoveContext(context.Background(), name...)
}

// RemoveContext is an implementation of multifs.FS
func (f FS) RemoveContext(ctx context.Context, name ...string) error {
	var err error

	for _, n := range name {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			errn := os.Remove(n)
			if errn != nil && errn != os.ErrNotExist {
				err = errors.Join(err, errn)
			}
		}
	}

	return err
}

// File is a wrapper to make os.File implement multifs.FS
type File struct {
	*os.File
}

// ReadContext is an implenetation of multifs.File
func (f *File) ReadContext(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.Read(p)
	}
}

// WriteAtContext is an implenetation of multifs.File
func (f *File) ReadAtContext(ctx context.Context, b []byte, off int64) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.ReadAt(b, off)
	}
}

// WriteContext is an implenetation of multifs.File
func (f *File) WriteContext(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.Write(p)
	}
}

// WriteAtContext is an implenetation of multifs.File
func (f *File) WriteAtContext(ctx context.Context, b []byte, off int64) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return f.WriteAt(b, off)
	}
}
