// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientutil

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ParseContentLength returns the content length (in bytes) parsed from the Content-Length
// HTTP header in input.
func ParseContentLength(m http.Header) (int64, error) {
	const name = "Content-Length"

	v, ok := m[name]
	if !ok {
		return 0, errors.Errorf("%s header not found", name)
	}

	if len(v) == 0 {
		return 0, errors.Errorf("%s header has no values", name)
	}

	ret, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "convert %s", name)
	}

	return ret, nil
}

// ParseLastModified returns the timestamp parsed from the Last-Modified
// HTTP header in input.
// Passing an second parameter, named f, to specify the time format.
// If f is empty then RFC3339 will be used as default format.
func ParseLastModified(m http.Header, f string) (time.Time, error) {
	const (
		name          = "Last-Modified"
		defaultFormat = time.RFC3339
	)

	v, ok := m[name]
	if !ok {
		return time.Time{}, errors.Errorf("%s header not found", name)
	}

	if len(v) == 0 {
		return time.Time{}, errors.Errorf("%s header has no values", name)
	}

	if f == "" {
		f = defaultFormat
	}

	mod, err := time.Parse(f, v[0])
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "parse %s", name)
	}

	return mod, nil
}

// ParseMD5 returns the bytes parsed from the hex-encoded MD5 string
// It trims potential surrounding double quotes before decoding.
// It returns nil if the MD5 string is not valid.
func ParseMD5(md5Hex string) []byte {
	// Trim surrounding double quotes if present.
	trimmed := strings.Trim(md5Hex, "\"")
	decoded, _ := hex.DecodeString(trimmed)
	if len(decoded) != 16 {
		return nil
	}
	return decoded
}
