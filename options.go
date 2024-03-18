package multifs

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"io"
	"io/fs"
)

type (
	Option func(*OptionWrapper) error

	byteProcessor  func([]byte) (int, error)
	readProcessor  func(io.Reader) io.Reader
	writeProcessor func(io.Writer) io.Writer
)

type OptionWrapper struct {
	fs              FS
	readProcessors  []readProcessor
	writeProcessors []writeProcessor
}

func (w *OptionWrapper) Open(name string) (File, error) {
	return w.OpenFile(name, O_RDONLY, 0)
}

func (w *OptionWrapper) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	file, err := w.fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &FileWrapper{
		File: file,
		fs:   w,
	}, nil
}

func (w *OptionWrapper) Mkdir(name string, perm fs.FileMode) error {
	return w.fs.Mkdir(name, perm)
}

func (w *OptionWrapper) MkdirAll(path string, perm fs.FileMode) error {
	return w.fs.MkdirAll(path, perm)
}

func (w *OptionWrapper) Remove(name ...string) error {
	return w.fs.Remove(name...)
}

func (w OptionWrapper) WithContext(ctx context.Context) OptionWrapper {
	w.fs = w.fs.WithContext(ctx)

	return w
}

type FileWrapper struct {
	File
	fs *OptionWrapper
}

func newWrapperFile(f File, fs *OptionWrapper) FileWrapper {
	return FileWrapper{
		File: f,
		fs:   fs,
	}
}

func (f FileWrapper) WithContext(ctx context.Context) File {
	fc := f
	fc.File = f.File.WithContext(ctx)

	return &fc
}

func (f *FileWrapper) Read(p []byte) (int, error) {
	var reader io.Reader = f.File

	for _, fn := range f.fs.readProcessors {
		reader = fn(reader)
	}

	return reader.Read(p)
}

func (f *FileWrapper) Write(p []byte) (int, error) {
	var writer io.Writer = f.File

	for _, fn := range f.fs.writeProcessors {
		writer = fn(writer)
	}

	return writer.Write(p)
}

// WithAESEncryption encrypts and decrypts file when reading/writing
func WithAESEcryption(key []byte) Option {
	return func(wrapper *OptionWrapper) error {
		block, err := aes.NewCipher(key)
		if err != nil {
			return err
		}
		wrapper.readProcessors = append(wrapper.readProcessors, func(r io.Reader) io.Reader {
			var iv [aes.BlockSize]byte
			stream := cipher.NewOFB(block, iv[:])
			return &cipher.StreamReader{S: stream, R: r}
		})

		wrapper.writeProcessors = append(wrapper.writeProcessors, func(w io.Writer) io.Writer {
			var iv [aes.BlockSize]byte
			stream := cipher.NewOFB(block, iv[:])
			return &cipher.StreamWriter{S: stream, W: w}
		})

		return nil
	}
}
