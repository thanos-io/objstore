package objstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestVisualizedBucketWrite(t *testing.T) {
	t.Skip("interactive test")

	b := newBucketCallsImage("test-object.txt", 1024)
	b.maxReads = 10

	// Add some test data - simulate reads on different parts of the object
	for i := 0; i < gridCellCount; i++ {
		if i%5 == 0 {
			b.rectangleGrid[i].reads = 1 // Single read (green)
		} else if i%15 == 0 {
			b.rectangleGrid[i].reads = 10 // Max reads (red)
		} else if i%25 == 0 {
			b.rectangleGrid[i].reads = 5 // Medium reads (yellow)
		}
	}

	b.renderGrid()
	testutil.Ok(t, b.write("/tmp/test_visualization.png"))
}

func TestVisualizedBucketDump(t *testing.T) {
	//t.Skip("interactive test")

	// Create a temp directory for the test
	tempDir, err := os.MkdirTemp("", "visualizedbucket-test")
	testutil.Ok(t, err)
	t.Logf("Find visualizations here: %s", tempDir)

	// Create a visualized bucket with test data
	inmemBkt := NewInMemBucket()
	vBkt := NewVisualizedBucket(inmemBkt)

	// Simulate some range reads on different objects
	ctx := context.Background()

	// Create some test objects
	testObjects := map[string][]byte{
		"object1.txt": make([]byte, 1000),
		"object2.txt": make([]byte, 2000),
		"object3.txt": make([]byte, 3000),
	}

	// Upload the test objects
	for name, content := range testObjects {
		testutil.Ok(t, inmemBkt.Upload(ctx, name, NopCloserWithSize(NewByteReader(content))))
	}

	// Perform multiple range reads with different patterns
	// Object 1: Read the beginning frequently
	for range 10 {
		_, err := vBkt.GetRange(ctx, "object1.txt", 0, 100)
		testutil.Ok(t, err)
	}
	// Read the middle less frequently
	for range 5 {
		_, err := vBkt.GetRange(ctx, "object1.txt", 400, 100)
		testutil.Ok(t, err)
	}
	// Read the end once
	_, err = vBkt.GetRange(ctx, "object1.txt", 900, 100)
	testutil.Ok(t, err)

	// Object 2: Evenly distributed reads
	for i := range 5 {
		_, err := vBkt.GetRange(ctx, "object2.txt", int64(i*400), 200)
		testutil.Ok(t, err)
	}

	// Object 3: Read just a small section in the middle
	_, err = vBkt.GetRange(ctx, "object3.txt", 1400, 200)
	testutil.Ok(t, err)

	// Generate the visualizations
	testutil.Ok(t, vBkt.Dump(filepath.Join(tempDir, "visualizations")))

	// Verify files were created
	for name := range testObjects {
		safeName := filepath.Join(tempDir, "visualizations", name+".png")
		_, err := os.Stat(safeName)
		testutil.Ok(t, err)
	}

	// Check index file
	_, err = os.Stat(filepath.Join(tempDir, "visualizations", "index.html"))
	testutil.Ok(t, err)
}

// Simple helper for creating a reader from byte slice
type byteReader struct {
	*strings.Reader
}

func NewByteReader(b []byte) *byteReader {
	return &byteReader{Reader: strings.NewReader(string(b))}
}

func (b *byteReader) ObjectSize() (int64, error) {
	return int64(b.Len()), nil
}
