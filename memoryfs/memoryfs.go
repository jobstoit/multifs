// Package memoryfs
//
// Recommended to use tempfs instead
package memoryfs

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/jobstoit/multifs"
)

// FS implements multifs.FS
type FS struct {
	files map[string]*File
}

// New returns a new in memmory filesystem
func New() *FS {
	return &FS{}
}

// Open is an implementation of multifs.FS
func (f *FS) Open(name string) (*File, error) {
	return f.OpenFileContext(context.Background(), name, multifs.O_RDONLY, 0)
}

// OpenContext is an implementation of multifs.FS
func (f *FS) OpenContext(ctx context.Context, name string) (*File, error) {
	return f.OpenFileContext(ctx, name, multifs.O_RDONLY, 0)
}

// OpenFile is an implementation of multifs.FS
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (*File, error) {
	return f.OpenFileContext(context.Background(), name, flag, perm)
}

// OpenFileContext is an implementation of multifs.FS
func (f *FS) OpenFileContext(ctx context.Context, name string, flag int, perm fs.FileMode) (*File, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return f.openFile(name, flag, perm)
	}
}

func (f *FS) openFile(name string, flag int, perm fs.FileMode) (*File, error) {
	fi, ok := f.files[name]
	if ok && flag&multifs.O_EXCL != 0 {
		return nil, fs.ErrExist
	}

	if !ok && flag&multifs.O_CREATE != 0 {
		return nil, fs.ErrNotExist
	}

	if flag&multifs.O_TRUNC != 0 || (!ok && flag&multifs.O_CREATE != 0) {
		fi = &File{
			key:          name,
			isDir:        false,
			lastModified: time.Now(),
		}

		f.files[name] = fi
	}

	f.files[name].flag = flag
	f.files[name].perm = perm
	if s := f.files[name].sys; s == nil {
		f.files[name].sys = fi
	}

	return fi, nil
}

// Remove is an implementation of multifs.FS
func (f *FS) Remove(name ...string) error {
	return f.RemoveContext(context.Background(), name...)
}

// RemoveContext is an implementation of multifs.FS
func (f *FS) RemoveContext(ctx context.Context, name ...string) error {
	var errs error
	for _, n := range name {
		select {
		case <-ctx.Done():
			return errors.Join(errs, ctx.Err())
		default:
			if _, ok := f.files[n]; !ok {
				errs = errors.Join(errs, fs.ErrNotExist)
				continue
			}

			delete(f.files, n)
		}
	}

	return errs
}

// File implements multifs.File
type File struct {
	key          string
	isDir        bool
	lastModified time.Time
	content      []byte
	flag         int
	perm         fs.FileMode
	sys          any
}

// Stat is an implementation of multifs.File
func (f *File) Stat() (fs.FileInfo, error) {
	return f.StatContext(context.Background())
}

// StatContext is an implementation of multifs.File
func (f *File) StatContext(ctx context.Context) (fs.FileInfo, error) {
	return multifs.NewFileInfo(
		f.key,
		int64(len(f.content)),
		f.perm,
		f.lastModified,
		f.isDir,
		f.sys,
	), nil
}

// Read is an implementation of multifs.File
func (f *File) Read(b []byte) (int, error) {
	return f.ReadAtContext(context.Background(), b, 0)
}

// ReadContext is an implementation of multifs.File
func (f *File) ReadContext(ctx context.Context, b []byte) (int, error) {
	return f.ReadAtContext(ctx, b, 0)
}

// CLose is an implementationf of multifs.File
func (f *File) Close() error {
	f.sys = nil
	return nil
}

// Write is an implementation of multifs.File
func (f *File) Write(b []byte) (int, error) {
	return f.WriteAtContext(context.Background(), b, 0)
}

// WriteContext is an implementation of multifs.File
func (f *File) WriteContext(ctx context.Context, b []byte) (int, error) {
	return f.WriteAtContext(ctx, b, 0)
}

// ReadAt is an implementation of multifs.File
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	return f.ReadAtContext(context.Background(), b, off)
}

// ReadAtContext is an implementation of multifs.File
func (f *File) ReadAtContext(ctx context.Context, b []byte, off int64) (int, error) {
	if f.sys == nil {
		return 0, fs.ErrClosed
	}

	if f.flag&multifs.O_RDWR+f.flag&multifs.O_RDONLY == 0 {
		return 0, fs.ErrPermission
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		if int(off) > len(f.content) {
			return 0, nil
		}

		return copy(b, f.content[off:]), nil
	}
}

// WriteAt is an implementation of multifs.File
func (f *File) WriteAt(b []byte, off int64) (int, error) {
	return f.WriteAtContext(context.Background(), b, off)
}

// WriteAtContext is an implentation of multifs.File
func (f *File) WriteAtContext(ctx context.Context, b []byte, off int64) (int, error) {
	if f.sys == nil {
		return 0, fs.ErrClosed
	}

	if f.flag&multifs.O_RDWR+f.flag&multifs.O_WRONLY == 0 {
		return 0, fs.ErrPermission
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		currLen := len(f.content)
		if currLen < int(off) {
			f.content = append(f.content, make([]byte, int(off)-currLen)...)

		}

		appendEnd := int(off) + len(b)

		suf := []byte{}
		if currLen > appendEnd {
			suf = f.content[appendEnd:]
		}

		f.content = append(f.content[:off], b...)
		f.content = append(f.content, suf...)

		return len(b), nil
	}
}
