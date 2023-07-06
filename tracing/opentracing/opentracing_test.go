// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package opentracing

import (
	"bytes"
	"io"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/objstore"
)

func TestTracingReader(t *testing.T) {
	r := bytes.NewReader([]byte("hello world"))
	tr := newTracingReadCloser(objstore.NopCloserWithSize(r), nil)

	size, err := objstore.TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)

	smallBuf := make([]byte, 4)
	n, err := io.ReadFull(tr, smallBuf)
	testutil.Ok(t, err)
	testutil.Equals(t, 4, n)

	// Verify that size is still the same, after reading 4 bytes.
	size, err = objstore.TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)
}
