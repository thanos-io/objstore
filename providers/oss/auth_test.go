package oss

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

func TestSdkAuth(t *testing.T) {
	ak := "test-access-key-id"
	os.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", ak)
	os.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "test-access-key-secret")
	defer func() {
		os.Unsetenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
		os.Unsetenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	}()

	logger := log.NewNopLogger()
	var gotReq *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	ts := server.Client().Transport

	b, err := NewBucketWithConfig(logger, Config{
		Endpoint: server.URL,
		Bucket:   "foobar",
		SdkAuth:  true,
	}, "", func(rt http.RoundTripper) http.RoundTripper {
		return ts
	})
	testutil.Ok(t, err, "Failed to create bucket with SDK auth")

	data, err := b.Get(context.TODO(), "test.txt")
	testutil.NotOk(t, err, "Expected error when accessing bucket with fake server")
	testutil.Assert(t, data == nil, "Expected no data, got: %v", data)

	testutil.Assert(t, gotReq != nil, "Expected request to be made, but got nil")
	testutil.Assert(t, len(gotReq.Header["Authorization"]) > 0,
		"Expected Authorization header to be set, but it was not")

	testutil.Assert(t, strings.Contains(gotReq.Header["Authorization"][0], ak),
		"Expected Authorization header to contain access key ID, but it did not: %s",
		gotReq.Header["Authorization"][0])
}

// fakeCredential is a credentials.Credential which records how many callers are
// inside GetCredential at the same time.
type fakeCredential struct {
	mu          sync.Mutex
	inFlight    int
	maxInFlight int
	calls       int
	err         error
}

func (c *fakeCredential) GetCredential() (*credentials.CredentialModel, error) {
	c.mu.Lock()
	c.inFlight++
	c.calls++
	if c.inFlight > c.maxInFlight {
		c.maxInFlight = c.inFlight
	}
	c.mu.Unlock()

	// Widen the window so that unserialized calls would overlap.
	time.Sleep(time.Millisecond)

	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()

	if c.err != nil {
		return nil, c.err
	}

	return &credentials.CredentialModel{
		AccessKeyId:     tea.String("test-access-key-id"),
		AccessKeySecret: tea.String("test-access-key-secret"),
		SecurityToken:   tea.String("test-security-token"),
	}, nil
}

func (c *fakeCredential) GetAccessKeyId() (*string, error) {
	return tea.String("test-access-key-id"), nil
}
func (c *fakeCredential) GetAccessKeySecret() (*string, error) {
	return tea.String("test-access-key-secret"), nil
}
func (c *fakeCredential) GetSecurityToken() (*string, error) {
	return tea.String("test-security-token"), nil
}
func (c *fakeCredential) GetBearerToken() *string { return nil }
func (c *fakeCredential) GetType() *string        { return tea.String("fake") }

func (c *fakeCredential) stats() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.maxInFlight
}

func TestCredentialsProviderGetCredentialsE(t *testing.T) {
	p := &CredentialsProvider{
		cred:   &fakeCredential{err: errors.New("sts is unavailable")},
		logger: log.NewNopLogger(),
	}

	cred, err := p.GetCredentialsE()
	testutil.NotOk(t, err, "Expected error, got none")
	testutil.Assert(t, cred == nil, "Expected nil credentials, got: %v", cred)
	testutil.Assert(t, strings.Contains(err.Error(), "sts is unavailable"),
		"Expected the underlying error to be preserved, got: %v", err)

	// The non-error variant is used by paths that cannot fail, it must not panic.
	got := p.GetCredentials()
	testutil.Equals(t, "", got.GetAccessKeyID())
	testutil.Equals(t, "", got.GetAccessKeySecret())
}

func TestCredentialsProviderGetCredentialsEConcurrency(t *testing.T) {
	const goroutines = 64
	fake := &fakeCredential{}
	p := &CredentialsProvider{cred: fake, logger: log.NewNopLogger()}

	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cred, err := p.GetCredentialsE()
			if err != nil {
				errCh <- err
				return
			}
			if cred.GetAccessKeyID() != "test-access-key-id" || cred.GetSecurityToken() != "test-security-token" {
				errCh <- errors.Errorf("unexpected credentials returned: %s/%s", cred.GetAccessKeyID(), cred.GetSecurityToken())
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		testutil.Ok(t, err)
	}

	calls, maxInFlight := fake.stats()
	testutil.Equals(t, goroutines, calls, "Expected every caller to fetch credentials")
	testutil.Equals(t, 1, maxInFlight, "Expected credential fetches to be serialized")
}
