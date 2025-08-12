// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientutil

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"
	"time"

	alioss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/efficientgo/core/testutil"
)

func TestParseLastModified(t *testing.T) {
	location, _ := time.LoadLocation("GMT")
	tests := map[string]struct {
		headerValue string
		expectedVal time.Time
		expectedErr string
		format      string
	}{
		"no header": {
			expectedErr: "Last-Modified header not found",
		},
		"empty format string to default RFC3339 format": {
			headerValue: "2015-11-06T10:07:11.000Z",
			expectedVal: time.Date(2015, time.November, 6, 10, 7, 11, 0, time.UTC),
			format:      "",
		},
		"valid RFC3339 header value": {
			headerValue: "2015-11-06T10:07:11.000Z",
			expectedVal: time.Date(2015, time.November, 6, 10, 7, 11, 0, time.UTC),
			format:      time.RFC3339,
		},
		"invalid RFC3339 header value": {
			headerValue: "invalid",
			expectedErr: `parse Last-Modified: parsing time "invalid" as "2006-01-02T15:04:05Z07:00": cannot parse "invalid" as "2006"`,
			format:      time.RFC3339,
		},
		"valid RFC1123 header value": {
			headerValue: "Fri, 24 Feb 2012 06:07:48 GMT",
			expectedVal: time.Date(2012, time.February, 24, 6, 7, 48, 0, location),
			format:      time.RFC1123,
		},
		"invalid RFC1123 header value": {
			headerValue: "invalid",
			expectedErr: `parse Last-Modified: parsing time "invalid" as "Mon, 02 Jan 2006 15:04:05 MST": cannot parse "invalid" as "Mon"`,
			format:      time.RFC1123,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			meta := http.Header{}
			if testData.headerValue != "" {
				meta.Add(alioss.HTTPHeaderLastModified, testData.headerValue)
			}

			actual, err := ParseLastModified(meta, testData.format)

			if testData.expectedErr != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, testData.expectedErr, err.Error())
			} else {
				testutil.Ok(t, err)
				testutil.Assert(t, testData.expectedVal.Equal(actual))
			}
		})
	}
}

func TestParseContentLength(t *testing.T) {
	tests := map[string]struct {
		headerValue string
		expectedVal int64
		expectedErr string
	}{
		"no header": {
			expectedErr: "Content-Length header not found",
		},
		"invalid header value": {
			headerValue: "invalid",
			expectedErr: `convert Content-Length: strconv.ParseInt: parsing "invalid": invalid syntax`,
		},
		"valid header value": {
			headerValue: "12345",
			expectedVal: 12345,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			meta := http.Header{}
			if testData.headerValue != "" {
				meta.Add(alioss.HTTPHeaderContentLength, testData.headerValue)
			}

			actual, err := ParseContentLength(meta)

			if testData.expectedErr != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, testData.expectedErr, err.Error())
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, testData.expectedVal, actual)
			}
		})
	}
}

func TestParseMD5(t *testing.T) {
	for _, tc := range []struct {
		label string
		in    string
		out   []byte
	}{
		{
			label: "valid md5",
			in:    "b1946ac92492d2347c6235b4d2611184",
			out:   []byte{0xb1, 0x94, 0x6a, 0xc9, 0x24, 0x92, 0xd2, 0x34, 0x7c, 0x62, 0x35, 0xb4, 0xd2, 0x61, 0x11, 0x84},
		},
		{
			label: "invalid hex string",
			in:    "not-a-hex-string",
		},
		{
			label: "odd length hex string",
			in:    "abc",
		},
		{
			label: "empty string",
			in:    "",
			out:   []byte{},
		},
		{
			label: "valid md5 with quotes",
			in:    "\"b1946ac92492d2347c6235b4d2611184\"",
			out:   []byte{0xb1, 0x94, 0x6a, 0xc9, 0x24, 0x92, 0xd2, 0x34, 0x7c, 0x62, 0x35, 0xb4, 0xd2, 0x61, 0x11, 0x84},
		},
		{
			label: "invalid hex string with quotes",
			in:    "\"not-a-hex-string\"",
		},
		{
			label: "only quotes",
			in:    "\"\"",
			out:   []byte{},
		},
	} {
		t.Run(tc.label, func(t *testing.T) {
			out := ParseMD5(tc.in)
			testutil.Assert(t, bytes.Equal(out, tc.out), fmt.Sprintf("output mismatch: %v != %v", out, tc.out))
		})
	}
}
