package multifs

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"io/fs"
)

type (
	Option func(*OptionsWrapper) error

	byteProcessor func([]byte) ([]byte, error)
)

type OptionWrapper struct {
	fs              FS
	readProcessors  []byteProcessor
	writeProcessors []byteProcessor
}

func (w *OptionWrapper) Open(name string) (File, error) {
	return w.OpenContext(context.Background(), name)
}

func (w *OptionWrapper) OpenContext(ctx context.Context, name string) (File, error) {
	return newWrapperFile(w.fs.OpenContext(ctx, name), w)
}

func (w *OptionWrapper) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	return w.OpenFileContext(context.Background(), name, flag, perm)
}

func (w *OptionWrapper) OpenFileContext(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	return newWrapperFile(w.fs.OpenFileContext(ctx, name, flag, perm), w)
}

func (w *OptionWrapper) Mkdir(name string, perm fs.FileMode) error {
	return w.MkdirContext(context.Background())
}

func (w *OptionWrapper) MkdirContext(ctx context.Context, name string, perm fs.FileMode) error {
	return w.fs.MkdirContext(ctx, name, perm)
}

func (w *OptionWrapper) MkdirAll(path string, perm fs.FileMode) error {
	return w.MkdirAllContext(context.Background(), path, perm)
}

func (w *OptionWrapper) MkdirAllContext(ctx context.Context, path string, perm fs.FileMode) error {
	return w.fs.MkdirAllContext(ctx, path, perm)
}

func (w *OptionWrapper) Remove(name ...string) error {
	return w.RemoveContext(context.Background(), name...)
}

func (w *OptionWrapper) RemoveContext(ctx context.Context, name ...string) error {
	return w.fs.RemoveContext(ctx, name...)
}

type FileWrapper struct {
	File
	fs *OptionWrapper
}

func newWrapperFile(f File, fs *OptionWrapper) FileWrapper {
	return FileWrapper{
		File: f,
		fs,
	}
}

func (f *FileWrapper) Read(b []byte) (int, error) {
	return f.ReadContext(context.Background(), b)
}

func (f *FileWrapper) ReadContext(ctx context.Context, b []byte) (int, error) {
	src := []byte{}
	count, err := f.File.ReadContext(ctx, src)
	if err != nil {
		return 0, err
	}

	for _, fn := range f.fs.readProcessors {
		src, err = fn(src)
		if err != nil {
			return 0, err
		}
	}

	return copy(b, src)
}

func (f *FileWrapper) Write(b []byte) (int, error) {
	return f.WriteContext(context.Background(), b)
}

func (f *FileWrapper) WriteContext(ctx context.Context, b []byte) (int, error) {
	src := b
	var err error

	for _, fn := range f.fs.writeProcessors {
		src, err = fn(src)
		if err != nil {
			return 0, err
		}
	}

	return f.File.WriteContext(ctx, src)
}

// WithAESEncryption encrypts and decrypts file when reading/writing
func WithAESEcryption(key []byte) Option {
	block, err := aes.NewCipher(key)
	if err != nil {
		return b, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return b, err
	}

	gSize := gcm.NonceSize()

	return func(wrapper *OptionWrapper) {
		wrapper.readProcessors = append(wrapper.readProcessors, func(b []byte) ([]byte, error) {
			nonce := b[:gSize]
			cipherText := b[gSize:]

			plainBytes, err := gcm.Open(nil, nonce, cipherText, nil)
			if err != nil {
				return b, nil
			}

			return plainBytes
		})

		wrapper.writeProcessors = append(wrapper.writeProcessors, func(b []byte) ([]byte, error) {
			nonce := make([]byte, gSize)
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				return b, err
			}

			cipherText := gcm.Seal(nonce, nonce, b, nil)

			return cipherText
		})
	}
}
