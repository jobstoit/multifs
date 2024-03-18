package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jobstoit/multifs"
)

const defaultChunkLength = 1024 * 1024 * 5 // 5mb

// FS is an s3 implementation of multifs.FS
type FS struct {
	bucketname  *string
	context     context.Context
	cli         *s3.Client
	chunkSize   int
	parallelism int
}

// New returns a new s3 implementation for multifs.FS
func New(bucketname string, cli *s3.Client) *FS {
	return &FS{
		bucketname:  &bucketname,
		cli:         cli,
		chunkSize:   defaultChunkLength,
		parallelism: 5,
	}
}

func (f *FS) Context() context.Context {
	if f.context == nil {
		f.context = context.Background()
	}

	return f.context
}

func (f FS) WithContext(ctx context.Context) multifs.FS {
	fc := f
	fc.context = ctx

	return &fc
}

// Open is an implementation of multifs.FS
func (f *FS) Open(name string) (multifs.File, error) {
	return f.OpenFile(name, multifs.O_RDONLY, 0)
}

// OpenFile is an implenetation of multifs.FS
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (multifs.File, error) {
	ctx := f.Context()

	file := &File{
		context: ctx,
		fs:      f,
		key:     &name,
		flag:    flag,
		mutex:   &sync.Mutex{},
	}

	exists, err := file.exists(ctx)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	if !exists && flag&multifs.O_CREATE > 0 {
		if err := f.createFile(ctx, name); err != nil {
			return nil, fs.ErrInvalid
		}
	}

	if flag&multifs.O_TRUNC > 0 {
		if err := f.createFile(ctx, name); err != nil {
			return nil, fs.ErrInvalid
		}
	}

	return file, nil
}

func (f *File) exists(ctx context.Context) (bool, error) {
	_, err := f.fs.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: f.fs.bucketname,
		Key:    f.key,
	})
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				return false, nil
			default:
				return false, err
			}
		}

		return false, err
	}

	return true, nil
}

func (f *FS) createFile(ctx context.Context, name string) error {
	_, err := f.cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: f.bucketname,
		Key:    &name,
		Body:   bytes.NewBufferString(""),
	})

	return err
}

// Mkdir is an implementaion of multifs.FS
func (f *FS) Mkdir(name string, perm fs.FileMode) error {
	return multifs.ErrNotImplemented
}

// MkdirAll is an implementaion of multifs.FS
func (f *FS) MkdirAll(name string, perm fs.FileMode) error {
	return multifs.ErrNotImplemented
}

// Remove is an implementation of multifs.FS
func (f *FS) Remove(name ...string) error {
	ctx := f.context
	toDelete := &types.Delete{}

	for _, n := range name {
		key := n
		toDelete.Objects = append(toDelete.Objects, types.ObjectIdentifier{
			Key: &key,
		})
	}

	_, err := f.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: f.bucketname,
		Delete: toDelete,
	})

	return err
}

// File is an s3 implementation for multifs.File
type File struct {
	context context.Context
	fs      *FS
	key     *string
	flag    int
	mutex   *sync.Mutex

	uploadCache *uploadCache
}

type uploadCache struct {
	uploadID      *string
	partNr        int32
	partMut       sync.Mutex
	completedPart []types.CompletedPart
	buf           []byte
	err           error
	wg            sync.WaitGroup
}

func (u *uploadCache) getPartNr() int32 {
	u.partMut.Lock()
	defer u.partMut.Unlock()
	u.partNr++

	return u.partNr
}

func newUploadCache(uploadID *string, bufSize int) *uploadCache {
	return &uploadCache{
		uploadID:      uploadID,
		partNr:        0,
		completedPart: []types.CompletedPart{},
		buf:           make([]byte, 0, bufSize),
		wg:            sync.WaitGroup{},
	}
}

func (f File) WithContext(ctx context.Context) multifs.File {
	fc := f
	fc.context = ctx

	return &fc
}

func (f *File) Context() context.Context {
	if f.context != nil {
		f.context = context.Background()
	}

	return f.context
}

// Read is an implenentation of multifs.File
func (f *File) Read(p []byte) (int, error) {
	ctx, cancel := context.WithCancel(f.Context())
	defer cancel()
	if err := f.flush(f.Context()); err != nil {
		return 0, err
	}

	if f.fs == nil {
		return 0, fs.ErrClosed
	}

	if f.flag&multifs.O_RDWR+f.flag&multifs.O_RDONLY == 0 {
		return 0, fs.ErrPermission
	}

	chunks := make(chan chunk, f.fs.parallelism)
	count := f.getChunks(ctx, chunks)

	byteCount := 0
	seq := 0

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return byteCount, ctx.Err()
		case chunk := <-chunks:
			if chunk.err != nil {
				return byteCount, chunk.err
			}

			for {
				select {
				case <-ctx.Done():
					return byteCount, ctx.Err()
				default:
					if chunk.sequence == seq {
						goto AFTER_READ_SEQUENCE
					}
				}
			}

		AFTER_READ_SEQUENCE:
			count, err := chunk.ReadClose(p)
			if err != nil {
				return byteCount, err
			}

			byteCount += count
		}
	}

	return byteCount, io.EOF
}

type chunk struct {
	body     io.ReadCloser
	err      error
	sequence int
}

func (c *chunk) ReadClose(p []byte) (int, error) {
	defer c.body.Close()

	return c.body.Read(p)
}

func (f *File) getChunks(ctx context.Context, chunks chan chunk) int {
	head, err := f.fs.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Key:    f.key,
		Bucket: f.fs.bucketname,
	})
	if err != nil {
		chunks <- chunk{
			body:     nil,
			err:      err,
			sequence: 0,
		}

		return 1
	}

	cl := int(head.ContentLength)
	count := 0

	for i := 0; i < cl; i += f.fs.chunkSize {
		end := i + defaultChunkLength
		if end > cl {
			end = cl
		}

		go f.getChunk(ctx, chunks, count, i, end)
		count++
	}

	return count
}

func (f *File) getChunk(ctx context.Context, chunks chan chunk, seq, start, end int) {
	res, err := f.fs.cli.GetObject(ctx, &s3.GetObjectInput{
		Key:    f.key,
		Bucket: f.fs.bucketname,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})

	chunks <- chunk{
		sequence: seq,
		body:     res.Body,
		err:      err,
	}
}

// Close is an implementation of multifs.File
func (f *File) Close() error {
	if err := f.flush(f.Context()); err != nil {
		return err
	}

	f.fs = nil
	f.context = nil
	return nil
}

// Stat is an implenentation of multifs.File
func (f *File) Stat() (fs.FileInfo, error) {
	ctx := f.Context()
	if err := f.flush(f.Context()); err != nil {
		return nil, err
	}

	filename := path.Base(*f.key)

	if f.fs == nil {
		return nil, fs.ErrClosed
	}

	res, err := f.fs.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: f.fs.bucketname,
		Key:    f.key,
	})
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch err.(type) {
			case *types.NotFound:
				return multifs.NewFileInfo(
					filename,
					0,
					0777,
					time.Now(),
					false,
					f.fs,
				), nil
			default:
				return nil, err
			}
		}

		return nil, err
	}

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
	ctx := f.Context()

	if err := f.preUpload(ctx); err != nil {
		return 0, errors.Join(err, f.abortUpload(ctx))
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	for _, b := range p {
		f.uploadCache.buf = append(f.uploadCache.buf, b)
		if len(f.uploadCache.buf) == cap(f.uploadCache.buf) {
			go f.uploadPart(ctx, bytes.NewReader(f.uploadCache.buf))
			clear(f.uploadCache.buf)
		}
	}

	return len(p), nil
}

func (f *File) flush(ctx context.Context) error {
	if f.uploadCache == nil {
		return nil
	}

	// final buffer
	f.uploadPart(ctx, bytes.NewReader(f.uploadCache.buf))

	f.uploadCache.wg.Wait()

	f.mutex.Lock()
	defer f.mutex.Unlock()

	sort.Slice(f.uploadCache.completedPart, func(i, j int) bool {
		return f.uploadCache.completedPart[i].PartNumber < f.uploadCache.completedPart[j].PartNumber
	})

	log.Printf("flush part len: %d", len(f.uploadCache.completedPart))

	_, err := f.fs.cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   f.fs.bucketname,
		Key:      f.key,
		UploadId: f.uploadCache.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: f.uploadCache.completedPart,
		},
	})
	if err != nil {
		err = errors.Join(err, f.abortUpload(ctx))
	}

	f.uploadCache = nil

	return err
}

func (f *File) preUpload(ctx context.Context) error {
	if f.uploadCache != nil {
		return nil
	}

	if f.uploadCache != nil && f.uploadCache.err != nil {
		return f.uploadCache.err
	}

	if f.fs == nil {
		return fs.ErrClosed
	}

	if f.flag&multifs.O_RDWR+f.flag&multifs.O_WRONLY == 0 {
		return fs.ErrPermission
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.uploadCache != nil {
		return nil
	}

	res, err := f.fs.cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: f.fs.bucketname,
		Key:    f.key,
	})
	if err != nil {
		return err
	}

	f.uploadCache = newUploadCache(res.UploadId, f.fs.chunkSize)

	if f.flag&multifs.O_APPEND > 0 {
		headRes, err := f.fs.cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: f.fs.bucketname,
			Key:    f.key,
		})
		if err != nil {
			return err
		}

		contentSize := int(headRes.ContentLength)

		for i := 0; i <= contentSize; i += f.fs.chunkSize {
			end := i + f.fs.chunkSize
			if end > contentSize {
				end = contentSize
			}

			partNr := f.uploadCache.getPartNr()
			ranges := fmt.Sprintf("bytes=%d-%d", i, end)
			copyPath := path.Join(*f.fs.bucketname, *f.key)

			rc, err := f.fs.cli.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
				Bucket:          f.fs.bucketname,
				Key:             f.key,
				CopySource:      &copyPath,
				PartNumber:      partNr,
				CopySourceRange: &ranges,
				UploadId:        f.uploadCache.uploadID,
			})
			if err != nil {
				return err
			}

			f.uploadCache.completedPart = append(f.uploadCache.completedPart, types.CompletedPart{
				ETag:           rc.CopyPartResult.ETag,
				ChecksumCRC32:  rc.CopyPartResult.ChecksumCRC32,
				ChecksumCRC32C: rc.CopyPartResult.ChecksumCRC32C,
				ChecksumSHA1:   rc.CopyPartResult.ChecksumSHA1,
				ChecksumSHA256: rc.CopyPartResult.ChecksumSHA256,
				PartNumber:     partNr,
			})
		}
	}

	return nil
}

func (f *File) uploadPart(ctx context.Context, p io.ReadSeeker) {
	f.uploadCache.wg.Add(1)
	defer f.uploadCache.wg.Done()

	partNr := f.uploadCache.getPartNr()
	log.Printf("upload part %d", f.uploadCache.partNr)

	res, err := f.fs.cli.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     f.fs.bucketname,
		Key:        f.key,
		UploadId:   f.uploadCache.uploadID,
		Body:       p,
		PartNumber: partNr,
	})
	if err != nil {
		return
	}

	f.uploadCache.completedPart = append(f.uploadCache.completedPart, types.CompletedPart{
		ETag:           res.ETag,
		ChecksumSHA256: res.ChecksumSHA256,
		ChecksumSHA1:   res.ChecksumSHA1,
		ChecksumCRC32:  res.ChecksumCRC32,
		ChecksumCRC32C: res.ChecksumCRC32C,
		PartNumber:     partNr,
	})
}

func (f *File) abortUpload(ctx context.Context) error {
	_, err := f.fs.cli.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   f.fs.bucketname,
		Key:      f.key,
		UploadId: f.uploadCache.uploadID,
	})
	if err != nil {
		return err
	}

	f.uploadCache = nil
	// TODO: set mutex on file level not on cache

	return nil
}
