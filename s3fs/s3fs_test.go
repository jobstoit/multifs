package s3fs

import (
	"context"
	"io/fs"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jobstoit/multifs"
)

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
