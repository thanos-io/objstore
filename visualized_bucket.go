// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/math/fixed"
)

// Visualization constants
const (
	gridCellCount = 900 // Total number of cells in the grid (30x30)
	gridDimension = 30  // Dimension of the square grid (30x30)
)

type bucketCallsImage struct {
	img *image.RGBA

	rectangleGrid [gridCellCount]bucketRectangle
	maxReads      uint64
	objectSize    int64
	fileName      string
}

// Visualization works by dividing the file into a 16 x 16 grid (256 rectangles).
// Each rectangle represents a portion of the file.
// We track how many times each portion is accessed via GetRange calls.
// The colors range from green (accessed once) to red (most frequently accessed).

type bucketRectangle struct {
	x1, y1, x2, y2 int

	reads uint64
}

func (i *bucketCallsImage) addHeader(header string) {
	col := color.RGBA{0, 0, 0, 255}

	drawer := &font.Drawer{
		Dst:  i.img,
		Src:  image.NewUniform(col),
		Face: basicfont.Face7x13,
	}

	textWidth := drawer.MeasureString(header).Ceil()

	pt := fixed.Point26_6{
		X: (fixed.I(i.img.Rect.Size().X) - fixed.I(textWidth)) / 2,
		Y: fixed.I(20),
	}

	drawer.Dot = pt
	drawer.DrawString(header)
}

func (i *bucketCallsImage) write(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer file.Close()

	return png.Encode(file, i.img)
}

func (i *bucketCallsImage) renderGrid() {
	// Constants for grid rendering
	const (
		gridMarginTop  = 80
		gridMarginLeft = 80
		gridWidth      = 1400
		gridHeight     = 1400
		cellWidth      = gridWidth / gridDimension
		cellHeight     = gridHeight / gridDimension
	)

	// Draw object name
	i.addHeader(fmt.Sprintf("Object: %s (Size: %d bytes)", i.fileName, i.objectSize))

	// Initialize grid positions
	for y := 0; y < gridDimension; y++ {
		for x := 0; x < gridDimension; x++ {
			idx := y*gridDimension + x
			i.rectangleGrid[idx].x1 = gridMarginLeft + x*cellWidth
			i.rectangleGrid[idx].y1 = gridMarginTop + y*cellHeight
			i.rectangleGrid[idx].x2 = gridMarginLeft + (x+1)*cellWidth
			i.rectangleGrid[idx].y2 = gridMarginTop + (y+1)*cellHeight
		}
	}

	// Draw each cell with appropriate color
	for _, rect := range i.rectangleGrid {
		// Skip cells with no reads
		if rect.reads == 0 {
			continue
		}

		// Calculate color based on access frequency
		// Green (0, 255, 0) for single access
		// Red (255, 0, 0) for maximum access
		var cellColor color.RGBA
		if i.maxReads == 1 {
			cellColor = color.RGBA{0, 255, 0, 255} // All green if everything accessed once
		} else if rect.reads == 1 {
			cellColor = color.RGBA{0, 255, 0, 255} // Green for single access
		} else {
			// Calculate gradient between green and red
			intensity := float64(rect.reads) / float64(i.maxReads)
			red := uint8(intensity * 255)
			green := uint8((1 - intensity) * 255)
			cellColor = color.RGBA{red, green, 0, 255}
		}

		// Draw the rectangle
		rectangle := image.Rect(rect.x1, rect.y1, rect.x2, rect.y2)
		draw.Draw(i.img, rectangle, &image.Uniform{cellColor}, image.Point{}, draw.Src)

		// Add border
		borderColor := color.RGBA{0, 0, 0, 255}
		for x := rect.x1; x < rect.x2; x++ {
			i.img.Set(x, rect.y1, borderColor)
			i.img.Set(x, rect.y2-1, borderColor)
		}
		for y := rect.y1; y < rect.y2; y++ {
			i.img.Set(rect.x1, y, borderColor)
			i.img.Set(rect.x2-1, y, borderColor)
		}

		// Add read count in the middle of the cell if there's enough space
		if rect.reads > 0 && cellWidth > 15 && cellHeight > 15 {
			countText := fmt.Sprintf("%d", rect.reads)
			textColor := color.RGBA{50, 50, 50, 200} // Slightly lighter color and semi-transparent
			drawer := &font.Drawer{
				Dst:  i.img,
				Src:  image.NewUniform(textColor),
				Face: basicfont.Face7x13,
			}
			textWidth := drawer.MeasureString(countText).Ceil()

			// Only render text if it will reasonably fit within the cell (with 4px padding on each side)
			if textWidth <= cellWidth-8 {
				drawer.Dot = fixed.Point26_6{
					X: fixed.I((rect.x1 + rect.x2 - textWidth) / 2),
					Y: fixed.I((rect.y1 + rect.y2 + 10) / 2), // Adjust vertical positioning for smaller cells
				}
				drawer.DrawString(countText)
			}
		}
	}

	// Add legend at the bottom
	legendY := gridMarginTop + gridHeight + 40
	legendText := "Color legend: Green = 1 access, Red = most frequent access"
	drawer := &font.Drawer{
		Dst:  i.img,
		Src:  image.NewUniform(color.RGBA{0, 0, 0, 255}),
		Face: basicfont.Face7x13,
	}
	drawer.Dot = fixed.Point26_6{
		X: fixed.I(gridMarginLeft),
		Y: fixed.I(legendY),
	}
	drawer.DrawString(legendText)
}

func newBucketCallsImage(fileName string, objectSize int64) *bucketCallsImage {
	img := image.NewRGBA(image.Rect(0, 0, 1600, 1600))
	white := color.RGBA{255, 255, 255, 255}
	draw.Draw(img, img.Bounds(), &image.Uniform{white}, image.Point{}, draw.Src)

	result := &bucketCallsImage{
		img:        img,
		fileName:   fileName,
		objectSize: objectSize,
	}

	// Initialize all cells with zero reads
	for i := range result.rectangleGrid {
		result.rectangleGrid[i].reads = 0
	}

	return result
}

// VisualizedBucket is like a bucket but it also captures
// the access patterns that it later writes out
// in a given directory. This is useful if you are retrieving
// a bunch of information from a bucket and want to visualize
// it later to see what kind of optimizations could be made.
// It creates heatmap visualizations of byte range accesses
// with a green to red gradient based on access frequency.
type VisualizedBucket struct {
	sync.Mutex
	reads   map[string][]read
	sizes   map[string]int64
	maxSize int64
	bkt     Bucket
}

type read struct {
	offset int64
	length int64
}

func NewVisualizedBucket(bkt Bucket) *VisualizedBucket {
	vb := &VisualizedBucket{
		reads: make(map[string][]read),
		sizes: make(map[string]int64),
		bkt:   bkt,
	}

	// NOTE(GiedriusS): since it would be hard to update all bucket usage,
	// let's periodically dump the data.

	go func() {
		t := time.NewTicker(time.Minute)

		for range t.C {
			vb.Dump(filepath.Join("/tmp", time.Now().String()))
		}
	}()

	return vb
}

// Dump creates visualization images for all objects with recorded access patterns
// in the specified directory. The images show heatmaps of byte range accesses.
func (v *VisualizedBucket) Dump(dirPath string) error {
	v.Lock()
	defer func() {
		clear(v.reads)
		clear(v.sizes)
		v.Unlock()
	}()

	if len(v.reads) == 0 {
		return nil // No reads to visualize
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(dirPath, 0750); err != nil {
		return fmt.Errorf("creating output directory %s: %w", dirPath, err)
	}

	// Process each object that has recorded reads
	for objName, reads := range v.reads {
		// Get object size
		objectSize := v.sizes[objName]
		if objectSize == 0 {
			objectSize = v.maxSize // Use max size as fallback
			if objectSize == 0 {
				objectSize = 1024 * 1024 // Default size if unknown
			}
		}

		// Create a new image for this object
		img := newBucketCallsImage(objName, objectSize)

		// Process the read ranges to determine grid cell access counts
		for _, r := range reads {
			// Calculate which cells this read covers
			startPercent := float64(r.offset) / float64(objectSize)
			endPercent := float64(r.offset+r.length) / float64(objectSize)

			startCell := int(startPercent * float64(gridCellCount))
			endCell := int(endPercent * float64(gridCellCount))

			// Handle edge cases
			if startCell < 0 {
				startCell = 0
			}
			if endCell >= gridCellCount {
				endCell = gridCellCount - 1
			}
			if startCell > gridCellCount-1 {
				startCell = gridCellCount - 1
			}

			// Increment read count for each cell this read touches
			for cell := startCell; cell <= endCell; cell++ {
				img.rectangleGrid[cell].reads++
				if img.rectangleGrid[cell].reads > img.maxReads {
					img.maxReads = img.rectangleGrid[cell].reads
				}
			}
		}

		// Render the grid with coloring
		img.renderGrid()

		// Create safe filename
		safeName := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
				return r
			}
			return '_'
		}, objName)

		// Write the image
		imagePath := filepath.Join(dirPath, fmt.Sprintf("%s.png", safeName))
		if err := img.write(imagePath); err != nil {
			return fmt.Errorf("writing visualization for %s: %w", objName, err)
		}
	}

	// Create an index file to list all visualizations
	indexPath := filepath.Join(dirPath, "index.html")
	return v.writeIndexFile(indexPath, dirPath)
}

// writeIndexFile creates an HTML index listing all visualizations
func (v *VisualizedBucket) writeIndexFile(indexPath, dirPath string) error {
	f, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("creating index file: %w", err)
	}
	defer f.Close()

	// Write HTML header
	fmt.Fprintf(f, `<!DOCTYPE html>
<html>
<head>
  <title>Object Storage Access Visualizations</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { color: #333; }
    .viz-list { margin-top: 20px; }
    .viz-item { margin-bottom: 10px; }
    .viz-item a { color: #0066cc; text-decoration: none; }
    .viz-item a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <h1>Object Storage Access Visualizations</h1>
  <p>Generated visualizations of object access patterns with heat maps:</p>
  <div class="viz-list">
`)

	// List all PNG files in the directory
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("reading directory contents: %w", err)
	}

	// Add links to each visualization
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && strings.HasSuffix(name, ".png") {
			fmt.Fprintf(f, `    <div class="viz-item"><a href="%s">%s</a></div>
`, name, name)
		}
	}

	// Write HTML footer
	fmt.Fprintf(f, `  </div>
</body>
</html>
`)

	return nil
}

func (v *VisualizedBucket) Provider() ObjProvider { return v.bkt.Provider() }

func (v *VisualizedBucket) Close() error {
	return v.bkt.Close()
}

func (v *VisualizedBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...IterOption) error {
	return v.bkt.Iter(ctx, dir, f, options...)
}

func (v *VisualizedBucket) IterWithAttributes(ctx context.Context, dir string, f func(IterObjectAttributes) error, options ...IterOption) error {
	return v.bkt.IterWithAttributes(ctx, dir, f, options...)
}

func (v *VisualizedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return v.bkt.Get(ctx, name)
}

func (v *VisualizedBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	// Record this read operation
	v.Lock()
	_, readsExist := v.reads[name]
	if readsExist {
		v.reads[name] = append(v.reads[name], read{
			offset: off,
			length: length,
		})
	} else {
		v.reads[name] = []read{
			{
				offset: off,
				length: length,
			},
		}
	}

	// Try to determine object size if we don't have it yet
	_, sizeExists := v.sizes[name]
	if !sizeExists {
		// Try to get attributes to determine size
		attrs, err := v.bkt.Attributes(ctx, name)
		if err == nil && attrs.Size > 0 {
			v.sizes[name] = attrs.Size
			if attrs.Size > v.maxSize {
				v.maxSize = attrs.Size
			}
		}
	}
	v.Unlock()

	// Forward the request to the actual bucket
	return v.bkt.GetRange(ctx, name, off, length)
}

func (v *VisualizedBucket) Exists(ctx context.Context, name string) (bool, error) {
	return v.bkt.Exists(ctx, name)
}

// Methods required by BucketReader interface
func (v *VisualizedBucket) SupportedIterOptions() []IterOptionType {
	return v.bkt.SupportedIterOptions()
}

func (v *VisualizedBucket) IsObjNotFoundErr(err error) bool {
	return v.bkt.IsObjNotFoundErr(err)
}

func (v *VisualizedBucket) IsAccessDeniedErr(err error) bool {
	return v.bkt.IsAccessDeniedErr(err)
}

// Methods required by Bucket interface
func (v *VisualizedBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	// Try to get the object size first
	if sizer, ok := r.(ObjectSizer); ok {
		if size, err := sizer.ObjectSize(); err == nil && size > 0 {
			v.Lock()
			v.sizes[name] = size
			if size > v.maxSize {
				v.maxSize = size
			}
			v.Unlock()
		}
	}

	return v.bkt.Upload(ctx, name, r)
}

func (v *VisualizedBucket) Delete(ctx context.Context, name string) error {
	// Remove any tracking data for this object
	v.Lock()
	delete(v.reads, name)
	delete(v.sizes, name)
	v.Unlock()

	return v.bkt.Delete(ctx, name)
}

func (v *VisualizedBucket) Name() string {
	return v.bkt.Name()
}
