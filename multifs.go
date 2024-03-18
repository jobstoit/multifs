// Package multifs extends io/fs with write opperations and implements
// various (virtual) files systems such as local, temporary, s3, sftp, webdav, etc.
//
// Mulitfs is a an abstraction over all these various filesystems and allows to write
// to multiple at the same time
package multifs

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"time"
)

const (
	ErrNotImplemented MultiFSError = "not implemented"
)

const (
	O_APPEND = os.O_APPEND
	O_CREATE = os.O_CREATE
	O_EXCL   = os.O_EXCL
	O_RDONLY = os.O_RDONLY
	O_RDWR   = os.O_RDWR
	O_SYNC   = os.O_SYNC
	O_TRUNC  = os.O_TRUNC
	O_WRONLY = os.O_WRONLY
)

type MultiFSError string

func (e MultiFSError) Error() string {
	return string(e)
}

// FS is an implementation of and extends io/fs.FS
// with writing and context capabilities
type FS interface {
	Open(name string) (File, error)
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	Mkdir(name string, perm fs.FileMode) error
	MkdirAll(path string, perm fs.FileMode) error
	Remove(name ...string) error
	WithContext(ctx context.Context) FS
}

// File is an implementation of and extends io/fs.File
// with writing and context capabilities
type File interface {
	fs.File
	io.Writer
	WithContext(ctx context.Context) File
}

// FileInfo implements fs.FileInfo and
type FileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
	sys     any
}

func NewFileInfo(name string, size int64, mode fs.FileMode, modtime time.Time, isDir bool, sys any) fs.FileInfo {
	return FileInfo{
		name,
		size,
		mode,
		modtime,
		isDir,
		sys,
	}
}

// Name is an implementation of fs.FileInfo
func (i FileInfo) Name() string {
	return i.name
}

// Size is an implementation of fs.FileInfo
func (i FileInfo) Size() int64 {
	return i.size
}

// Mode is an implementation of fs.FileInfo
func (i FileInfo) Mode() fs.FileMode {
	return i.mode
}

// ModTime is an implementation of fs.FileInfo
func (i FileInfo) ModTime() time.Time {
	return i.modTime
}

// IsDir is an implementation of fs.FileInfo
func (i FileInfo) IsDir() bool {
	return i.isDir
}

// Sys is an implementation of fs.FileInfo
func (i FileInfo) Sys() any {
	return i.sys
}

// ByteBuffer is a bytes buffer that contains a WriteAt and ReadAt function
type ByteBuffer struct {
	*bytes.Buffer
}

func NewByteBuffer(buf []byte) *ByteBuffer {
	return &ByteBuffer{
		Buffer: bytes.NewBuffer(buf),
	}
}

// WriteAt is an implementation of io.WriterAt
func (w *ByteBuffer) WriteAt(b []byte, off int64) (int, error) {
	newBytes := w.Bytes()

	writeLen := len(b)
	if sufix := (int(off) + writeLen) - w.Len(); sufix > 0 {
		newBytes = append(newBytes, make([]byte, sufix)...)
	}

	for _, wb := range b {
		newBytes[off] = wb
		off++
	}

	w.Reset()
	_, err := w.Write(newBytes)

	return writeLen, err
}

// ReadAt is an implementation of io.ReaderAt
func (w *ByteBuffer) ReadAt(b []byte, off int64) (int, error) {
	if int(off) > w.Len() {
		return 0, nil
	}

	return copy(b, w.Bytes()[off:]), nil
}

func (w *ByteBuffer) Clone() *ByteBuffer {
	return NewByteBuffer(w.Bytes())
}
