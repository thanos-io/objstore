// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestInMem_ReturnsModifiedInIterAttributes(t *testing.T) {
	b := NewInMemBucket()
	testutil.Ok(t, b.Upload(context.Background(), "test/file1.txt", strings.NewReader("test-data1")))

	var itemsIterated int

	testutil.Ok(t, b.IterWithAttributes(context.Background(), "", func(attrs IterObjectAttributes) error {
		testutil.Equals(t, "test/", attrs.Name)
		ts, ok := attrs.LastModified()
		testutil.Equals(t, true, ok)
		testutil.Assert(t, !ts.IsZero(), "expected LastModified to be not zero")
		itemsIterated++

		return nil
	}, WithUpdatedAt()))

	testutil.Ok(t, b.IterWithAttributes(context.Background(), "", func(attrs IterObjectAttributes) error {
		testutil.Equals(t, "test/file1.txt", attrs.Name)

		ts, ok := attrs.LastModified()
		testutil.Equals(t, true, ok)
		testutil.Assert(t, !ts.IsZero(), "expected LastModified to be not zero")

		itemsIterated++

		return nil
	}, WithRecursiveIter(), WithUpdatedAt()))

	testutil.Equals(t, 2, itemsIterated)
}
