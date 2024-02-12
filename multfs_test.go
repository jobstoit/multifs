package multifs

import (
	"io"
	"log"
	"os"
	"testing"
)

func TestWriting(t *testing.T) {
	//buf := &bytes.Buffer{}
	f, err := os.OpenFile("/tmp/test.txt", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	f.Write([]byte("hello"))

	b, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	log.Printf("content: %s", b)

	f.Write([]byte("hello2"))

	b, err = io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	log.Printf("content2: %s", b)
}
