package s3fs

import (
	"context"
	"embed"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jobstoit/multifs"
)

//go:embed testdata
var assets embed.FS

func TestCompatibility(t *testing.T) {
	var fileSystem multifs.FS

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Errorf("error loading s3 config: %v", err)
	}

	cli := s3.NewFromConfig(cfg)

	fileSystem = New("testing", cli)
	_ = fileSystem.Mkdir("testing_dir/", fs.ModeDir)
}

func TestFS(t *testing.T) {
	cli := getTestCli(t)
	fs := New("testing", cli)

	t.Run("Create file", func(t *testing.T) {
		file, err := fs.OpenFile("data/checks.png", multifs.O_WRONLY|multifs.O_CREATE|multifs.O_APPEND, 0)
		if err != nil {
			t.Errorf("error opening file: %v", err)
			t.FailNow()
		}

		a, err := assets.Open("testdata/checks.png")
		if err != nil {
			t.Errorf("error opening file: %v", err)
			t.FailNow()
		}
		defer a.Close()

		_, err = io.Copy(file, a)
		if err != nil {
			t.Errorf("error writing file: %v", err)
			t.Fail()
		}

		if err := file.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
			t.Fail()
		}
	})
}

func getTestCli(t *testing.T) *s3.Client {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("local"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: "http://localhost:9000"}, nil
		})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access_key", "secret_key", "")),
	)
	if err != nil {
		t.Errorf("error connecting with minio on url '%s': %v", "http://localhost:9000", err)
		t.FailNow()
	}

	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	_, err = cli.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("testing"),
	})
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				_, errC := cli.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("testing"),
				})
				if errC != nil {
					t.Errorf("error creating missing bucket: %v", err)
					t.FailNow()
				}
			default:
				t.Errorf("unable to get bucket details: %v", err)
				t.FailNow()
			}
		}
	}

	return cli
}
