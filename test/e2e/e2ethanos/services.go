// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/efficientgo/core/backoff"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/promconfig/discovery/targetgroup"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/exthttp"
	"github.com/thanos-io/objstore/providers/s3"
)

const (
	infoLogLevel = "info"
)

// Same as default for now.
var defaultBackoffConfig = backoff.Config{
	Min:        300 * time.Millisecond,
	Max:        600 * time.Millisecond,
	MaxRetries: 50,
}

// TODO(bwplotka): Make strconv.Itoa(os.Getuid()) pattern in e2e?
func wrapWithDefaults(opt e2e.StartOptions) e2e.StartOptions {
	if opt.User == "" {
		opt.User = strconv.Itoa(os.Getuid())
	}
	if opt.WaitReadyBackoff == nil {
		opt.WaitReadyBackoff = &defaultBackoffConfig
	}
	return opt
}

const (
	// FeatureExemplarStorage is a feature flag that enables exemplar storage on Prometheus.
	FeatureExemplarStorage = "exemplar-storage"
)

// DefaultPrometheusImage sets default Prometheus image used in e2e service.
func DefaultPrometheusImage() string {
	return "quay.io/prometheus/prometheus:v2.29.2"
}

// DefaultAlertmanagerImage sets default Alertmanager image used in e2e service.
func DefaultAlertmanagerImage() string {
	return "quay.io/prometheus/alertmanager:v0.20.0"
}

// DefaultImage returns the local docker image to use to run Thanos.
func DefaultImage() string {
	// Get the Thanos image from the THANOS_IMAGE env variable.
	if os.Getenv("THANOS_IMAGE") != "" {
		return os.Getenv("THANOS_IMAGE")
	}

	return "thanos"
}

func defaultPromHttpConfig() string {
	// username: test, secret: test(bcrypt hash)
	return `basic_auth:
  username: test
  password: test
`
}

func NewPrometheus(e e2e.Environment, name, promConfig, webConfig, promImage string, enableFeatures ...string) *e2emon.InstrumentedRunnable {
	f := e.Runnable(name).
		WithPorts(map[string]int{"http": 9090}).
		Future()

	if err := os.MkdirAll(f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create prometheus dir"))}
	}

	if err := os.WriteFile(filepath.Join(f.Dir(), "prometheus.yml"), []byte(promConfig), 0600); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating prom config"))}
	}

	if len(webConfig) > 0 {
		if err := os.WriteFile(filepath.Join(f.Dir(), "web-config.yml"), []byte(webConfig), 0600); err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating web-config"))}
		}
	}

	probe := e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200)
	args := e2e.BuildArgs(map[string]string{
		"--config.file":                     filepath.Join(f.InternalDir(), "prometheus.yml"),
		"--storage.tsdb.path":               f.InternalDir(),
		"--storage.tsdb.max-block-duration": "2h",
		"--log.level":                       infoLogLevel,
		"--web.listen-address":              ":9090",
	})

	if len(enableFeatures) > 0 {
		args = append(args, fmt.Sprintf("--enable-feature=%s", strings.Join(enableFeatures, ",")))
	}
	if len(webConfig) > 0 {
		args = append(args, fmt.Sprintf("--web.config.file=%s", filepath.Join(f.InternalDir(), "web-config.yml")))
		// If auth is enabled then prober would get 401 error.
		probe = e2e.NewHTTPReadinessProbe("http", "/-/ready", 401, 401)
	}
	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     promImage,
		Command:   e2e.NewCommandWithoutEntrypoint("prometheus", args...),
		Readiness: probe,
	})), "http")
}

func NewPrometheusWithSidecar(e e2e.Environment, name, promConfig, webConfig, promImage, minTime string, enableFeatures ...string) (*e2emon.InstrumentedRunnable, *e2emon.InstrumentedRunnable) {
	return NewPrometheusWithSidecarCustomImage(e, name, promConfig, webConfig, promImage, minTime, DefaultImage(), enableFeatures...)
}

func NewPrometheusWithSidecarCustomImage(e e2e.Environment, name, promConfig, webConfig, promImage, minTime string, sidecarImage string, enableFeatures ...string) (*e2emon.InstrumentedRunnable, *e2emon.InstrumentedRunnable) {
	prom := NewPrometheus(e, name, promConfig, webConfig, promImage, enableFeatures...)

	args := map[string]string{
		"--debug.name":        fmt.Sprintf("sidecar-%v", name),
		"--grpc-address":      ":9091",
		"--grpc-grace-period": "0s",
		"--http-address":      ":8080",
		"--prometheus.url":    "http://" + prom.InternalEndpoint("http"),
		"--tsdb.path":         prom.InternalDir(),
		"--log.level":         "debug",
	}
	if len(webConfig) > 0 {
		args["--prometheus.http-client"] = defaultPromHttpConfig()
	}
	if minTime != "" {
		args["--min-time"] = minTime
	}
	sidecarRunnable := e.Runnable(fmt.Sprintf("sidecar-%s", name)).WithPorts(map[string]int{"http": 8080, "grpc": 9091}).Init(wrapWithDefaults(e2e.StartOptions{
		Image:     sidecarImage,
		Command:   e2e.NewCommand("sidecar", e2e.BuildArgs(args)...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	}))
	sidecar := e2emon.AsInstrumented(sidecarRunnable, "http")
	return prom, sidecar
}

type QuerierBuilder struct {
	name           string
	routePrefix    string
	externalPrefix string
	image          string

	storeAddresses       []string
	fileSDStoreAddresses []string
	ruleAddresses        []string
	metadataAddresses    []string
	targetAddresses      []string
	exemplarAddresses    []string
	enableFeatures       []string
	endpoints            []string

	replicaLabels []string
	tracingConfig string

	e2e.Linkable
	f e2e.FutureRunnable
}

func NewQuerierBuilder(e e2e.Environment, name string, storeAddresses ...string) *QuerierBuilder {
	f := e.Runnable(fmt.Sprintf("querier-%v", name)).
		WithPorts(map[string]int{
			"http": 8080,
			"grpc": 9091,
		}).
		Future()
	return &QuerierBuilder{
		Linkable:       f,
		f:              f,
		name:           name,
		storeAddresses: storeAddresses,
		image:          DefaultImage(),
		replicaLabels:  []string{replicaLabel},
	}
}

func (q *QuerierBuilder) WithEnabledFeatures(enableFeatures []string) *QuerierBuilder {
	q.enableFeatures = enableFeatures
	return q
}

func (q *QuerierBuilder) WithImage(image string) *QuerierBuilder {
	q.image = image
	return q
}

func (q *QuerierBuilder) WithStoreAddresses(storeAddresses ...string) *QuerierBuilder {
	q.storeAddresses = storeAddresses
	return q
}

func (q *QuerierBuilder) WithFileSDStoreAddresses(fileSDStoreAddresses ...string) *QuerierBuilder {
	q.fileSDStoreAddresses = fileSDStoreAddresses
	return q
}

func (q *QuerierBuilder) WithRuleAddresses(ruleAddresses ...string) *QuerierBuilder {
	q.ruleAddresses = ruleAddresses
	return q
}

func (q *QuerierBuilder) WithTargetAddresses(targetAddresses ...string) *QuerierBuilder {
	q.targetAddresses = targetAddresses
	return q
}

func (q *QuerierBuilder) WithExemplarAddresses(exemplarAddresses ...string) *QuerierBuilder {
	q.exemplarAddresses = exemplarAddresses
	return q
}

func (q *QuerierBuilder) WithMetadataAddresses(metadataAddresses ...string) *QuerierBuilder {
	q.metadataAddresses = metadataAddresses
	return q
}

func (q *QuerierBuilder) WithEndpoints(endpoints ...string) *QuerierBuilder {
	q.endpoints = endpoints
	return q
}

func (q *QuerierBuilder) WithRoutePrefix(routePrefix string) *QuerierBuilder {
	q.routePrefix = routePrefix
	return q
}

func (q *QuerierBuilder) WithExternalPrefix(externalPrefix string) *QuerierBuilder {
	q.externalPrefix = externalPrefix
	return q
}

func (q *QuerierBuilder) WithTracingConfig(tracingConfig string) *QuerierBuilder {
	q.tracingConfig = tracingConfig
	return q
}

// WithReplicaLabels replaces default [replica] replica label configuration for the querier.
func (q *QuerierBuilder) WithReplicaLabels(labels ...string) *QuerierBuilder {
	q.replicaLabels = labels
	return q
}

func (q *QuerierBuilder) Init() *e2emon.InstrumentedRunnable {
	args, err := q.collectArgs()
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(q.name, err)}
	}

	return e2emon.AsInstrumented(q.f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     q.image,
		Command:   e2e.NewCommand("query", args...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

const replicaLabel = "replica"

func (q *QuerierBuilder) collectArgs() ([]string, error) {
	args := e2e.BuildArgs(map[string]string{
		"--debug.name":            fmt.Sprintf("querier-%v", q.name),
		"--grpc-address":          ":9091",
		"--grpc-grace-period":     "0s",
		"--http-address":          ":8080",
		"--store.sd-dns-interval": "5s",
		"--log.level":             infoLogLevel,
		"--query.max-concurrent":  "1",
		"--store.sd-interval":     "5s",
	})

	for _, repl := range q.replicaLabels {
		args = append(args, "--query.replica-label="+repl)
	}
	for _, addr := range q.storeAddresses {
		args = append(args, "--store="+addr)
	}
	for _, addr := range q.ruleAddresses {
		args = append(args, "--rule="+addr)
	}
	for _, addr := range q.targetAddresses {
		args = append(args, "--target="+addr)
	}
	for _, addr := range q.metadataAddresses {
		args = append(args, "--metadata="+addr)
	}
	for _, addr := range q.exemplarAddresses {
		args = append(args, "--exemplar="+addr)
	}
	for _, feature := range q.enableFeatures {
		args = append(args, "--enable-feature="+feature)
	}
	for _, addr := range q.endpoints {
		args = append(args, "--endpoint="+addr)
	}
	if len(q.fileSDStoreAddresses) > 0 {
		if err := os.MkdirAll(q.Dir(), 0750); err != nil {
			return nil, errors.Wrap(err, "create query dir failed")
		}

		fileSD := []*targetgroup.Group{{}}
		for _, a := range q.fileSDStoreAddresses {
			fileSD[0].Targets = append(fileSD[0].Targets, model.LabelSet{model.AddressLabel: model.LabelValue(a)})
		}

		b, err := yaml.Marshal(fileSD)
		if err != nil {
			return nil, err
		}

		if err := os.WriteFile(q.Dir()+"/filesd.yaml", b, 0600); err != nil {
			return nil, errors.Wrap(err, "creating query SD config failed")
		}

		args = append(args, "--store.sd-files="+filepath.Join(q.InternalDir(), "filesd.yaml"))
	}
	if q.routePrefix != "" {
		args = append(args, "--web.route-prefix="+q.routePrefix)
	}
	if q.externalPrefix != "" {
		args = append(args, "--web.external-prefix="+q.externalPrefix)
	}
	if q.tracingConfig != "" {
		args = append(args, "--tracing.config="+q.tracingConfig)
	}
	return args, nil
}

func NewReverseProxy(e e2e.Environment, name, tenantID, target string) *e2emon.InstrumentedRunnable {
	conf := fmt.Sprintf(`
events {
	worker_connections  1024;
}

http {
	server {
		listen 80;
		server_name _;

		location / {
			proxy_set_header THANOS-TENANT %s;
			proxy_pass %s;
		}
	}
}
`, tenantID, target)

	f := e.Runnable(fmt.Sprintf("nginx-%s", name)).
		WithPorts(map[string]int{"http": 80}).
		Future()

	if err := os.MkdirAll(f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create store dir"))}
	}

	if err := os.WriteFile(filepath.Join(f.Dir(), "nginx.conf"), []byte(conf), 0600); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating nginx config file failed"))}
	}

	return e2emon.AsInstrumented(f.Init(e2e.StartOptions{
		Image:            "docker.io/nginx:1.21.1-alpine",
		Volumes:          []string{filepath.Join(f.Dir(), "/nginx.conf") + ":/etc/nginx/nginx.conf:ro"},
		WaitReadyBackoff: &defaultBackoffConfig,
	}), "http")
}

// NewMinio returns minio server, used as a local replacement for S3.
// TODO(@matej-g): This is a temporary workaround for https://github.com/efficientgo/e2e/issues/11;
// after this is addresses fixed all calls should be replaced with e2edb.NewMinio.
func NewMinio(e e2e.Environment, name, bktName string) *e2emon.InstrumentedRunnable {
	image := "minio/minio:RELEASE.2022-07-30T05-21-40Z"
	minioKESGithubContent := "https://raw.githubusercontent.com/minio/kes/master"

	httpsPort := 8090
	consolePort := 8080
	f := e.Runnable(fmt.Sprintf("minio-%s", name)).
		WithPorts(map[string]int{"https": httpsPort, "console": consolePort}).
		Future()

	if err := os.MkdirAll(filepath.Join(f.Dir(), "certs", "CAs"), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create certs dir"))}
	}

	if err := genCerts(
		filepath.Join(f.Dir(), "certs", "public.crt"),
		filepath.Join(f.Dir(), "certs", "private.key"),
		filepath.Join(f.Dir(), "certs", "CAs", "ca.crt"),
		fmt.Sprintf("%s-minio-%s", e.Name(), name),
	); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "fail to generate certs"))}
	}

	commands := []string{
		fmt.Sprintf("curl -sSL --tlsv1.2 -O '%s/root.key' -O '%s/root.cert'", minioKESGithubContent, minioKESGithubContent),
		fmt.Sprintf("mkdir -p /data/%s && minio server --certs-dir %s/certs --address :%v --console-address :%v /data", bktName, f.InternalDir(), httpsPort, consolePort),
	}

	minio := e2emon.AsInstrumented(f.Init(e2e.StartOptions{
		Image: image,
		// Create the required bucket before starting minio.
		Command:   e2e.NewCommandWithoutEntrypoint("sh", "-c", strings.Join(commands, " && ")),
		Readiness: e2e.NewHTTPSReadinessProbe("console", "/", 200, 200),
		EnvVars: map[string]string{
			"MINIO_ROOT_USER":     e2edb.MinioAccessKey,
			"MINIO_ROOT_PASSWORD": e2edb.MinioSecretKey,
			"MINIO_BROWSER":       "on",
			"ENABLE_HTTPS":        "1",
			// https://docs.min.io/docs/minio-kms-quickstart-guide.html
			"MINIO_KMS_KES_ENDPOINT":  "https://play.min.io:7373",
			"MINIO_KMS_KES_KEY_FILE":  "root.key",
			"MINIO_KMS_KES_CERT_FILE": "root.cert",
			"MINIO_KMS_KES_KEY_NAME":  "my-minio-key",
		},
	}), "https")
	return minio
}

func NewMemcached(e e2e.Environment, name string) *e2emon.InstrumentedRunnable {
	return e2emon.AsInstrumented(e.Runnable(fmt.Sprintf("memcached-%s", name)).
		WithPorts(map[string]int{"memcached": 11211}).
		Init(e2e.StartOptions{
			Image:            "docker.io/memcached:1.6.3-alpine",
			Command:          e2e.NewCommand("memcached", []string{"-m 1024", "-I 1m", "-c 1024", "-v"}...),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		}), "memcached")
}

func NewToolsBucketWeb(
	e e2e.Environment,
	name string,
	bucketConfig client.BucketConfig,
	routePrefix,
	externalPrefix string,
	minTime string,
	maxTime string,
	relabelConfig string,
) *e2emon.InstrumentedRunnable {
	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrapf(err, "generate tools bucket web config file: %v", bucketConfig))}
	}

	f := e.Runnable(fmt.Sprintf("toolsBucketWeb-%s", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Future()

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":      fmt.Sprintf("toolsBucketWeb-%s", name),
		"--http-address":    ":8080",
		"--log.level":       infoLogLevel,
		"--objstore.config": string(bktConfigBytes),
	})
	if routePrefix != "" {
		args = append(args, "--web.route-prefix="+routePrefix)
	}

	if externalPrefix != "" {
		args = append(args, "--web.external-prefix="+externalPrefix)
	}

	if minTime != "" {
		args = append(args, "--min-time="+minTime)
	}

	if maxTime != "" {
		args = append(args, "--max-time="+maxTime)
	}

	if relabelConfig != "" {
		args = append(args, "--selector.relabel-config="+relabelConfig)
	}

	args = append([]string{"bucket", "web"}, args...)

	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     DefaultImage(),
		Command:   e2e.NewCommand("tools", args...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

// genCerts generates certificates and writes those to the provided paths.
func genCerts(certPath, privkeyPath, caPath, serverName string) error {
	var caRoot = &x509.Certificate{
		SerialNumber:          big.NewInt(2019),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	var cert = &x509.Certificate{
		SerialNumber: big.NewInt(1658),
		DNSNames:     []string{serverName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	// Generate CA cert.
	caBytes, err := x509.CreateCertificate(rand.Reader, caRoot, caRoot, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	err = os.WriteFile(caPath, caPEM, 0644)
	if err != nil {
		return err
	}

	// Sign the cert with the CA private key.
	certBytes, err := x509.CreateCertificate(rand.Reader, cert, caRoot, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	err = os.WriteFile(certPath, certPEM, 0644)
	if err != nil {
		return err
	}

	certPrivKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})
	err = os.WriteFile(privkeyPath, certPrivKeyPEM, 0644)
	if err != nil {
		return err
	}

	return nil
}

func NewS3Config(bucket, endpoint, basePath string) s3.Config {
	return s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  endpoint,
		Insecure:  false,
		HTTPConfig: exthttp.HTTPConfig{
			TLSConfig: exthttp.TLSConfig{
				CAFile:   filepath.Join(basePath, "certs", "CAs", "ca.crt"),
				CertFile: filepath.Join(basePath, "certs", "public.crt"),
				KeyFile:  filepath.Join(basePath, "certs", "private.key"),
			},
		},
		BucketLookupType: s3.AutoLookup,
	}
}

// NOTE: by using aggregation all results are now unsorted.
var QueryUpWithoutInstance = func() string { return "sum(up) without (instance)" }

// LocalPrometheusTarget is a constant to be used in the Prometheus config if you
// wish to enable Prometheus to scrape itself in a test.
const LocalPrometheusTarget = "localhost:9090"

// DefaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * optionallly scrape self. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func DefaultPromConfig(name string, replica int, remoteWriteEndpoint, ruleFile string, scrapeTargets ...string) string {
	var targets string
	if len(scrapeTargets) > 0 {
		targets = strings.Join(scrapeTargets, ",")
	}

	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
`, name, replica)

	if targets != "" {
		config = fmt.Sprintf(`
%s
scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: [%s]
  relabel_configs:
  - source_labels: ['__address__']
    regex: '^.+:80$'
    action: drop
`, config, targets)
	}

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:
- url: "%s"
  # Don't spam receiver on mistake.
  queue_config:
    min_backoff: 2s
    max_backoff: 10s
`, config, remoteWriteEndpoint)
	}

	if ruleFile != "" {
		config = fmt.Sprintf(`
%s
rule_files:
-  "%s"
`, config, ruleFile)
	}

	return config
}
