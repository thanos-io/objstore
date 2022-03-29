package azure

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
)

const (
	azureDefaultEndpoint = "blob.core.windows.net"
)

// Set default retry values to default Azure values. 0 = use Default Azure.
var DefaultConfig = Config{
	PipelineConfig: PipelineConfig{
		MaxTries:      0,
		TryTimeout:    10,
		RetryDelay:    0,
		MaxRetryDelay: 0,
	},
	ReaderConfig: ReaderConfig{
		MaxRetryRequests: 0,
	},
	HTTPConfig: HTTPConfig{
		IdleConnTimeout:       model.Duration(10 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   model.Duration(10 * time.Second),
		ExpectContinueTimeout: model.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
		DisableCompression:    false,
	},
}

// Config Azure storage configuration.
type Config struct {
	StorageAccountName string         `yaml:"storage_account"`
	StorageAccountKey  string         `yaml:"storage_account_key"`
	ContainerName      string         `yaml:"container"`
	Endpoint           string         `yaml:"endpoint"`
	MaxRetries         int            `yaml:"max_retries"`
	MSIResource        string         `yaml:"msi_resource"`
	UserAssignedID     string         `yaml:"user_assigned_id"`
	PipelineConfig     PipelineConfig `yaml:"pipeline_config"`
	ReaderConfig       ReaderConfig   `yaml:"reader_config"`
	HTTPConfig         HTTPConfig     `yaml:"http_config"`
}

type ReaderConfig struct {
	MaxRetryRequests int `yaml:"max_retry_requests"`
}

type PipelineConfig struct {
	MaxTries      int32          `yaml:"max_tries"`
	TryTimeout    model.Duration `yaml:"try_timeout"`
	RetryDelay    model.Duration `yaml:"retry_delay"`
	MaxRetryDelay model.Duration `yaml:"max_retry_delay"`
}

type HTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout model.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool           `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   model.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout model.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int            `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int            `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int            `yaml:"max_conns_per_host"`
	DisableCompression    bool           `yaml:"disable_compression"`

	TLSConfig objstore.TLSConfig `yaml:"tls_config"`
}

// Validate checks to see if any of the config options are set.
func (conf *Config) validate() error {
	var errMsg []string
	if conf.MSIResource == "" {
		if conf.UserAssignedID == "" {
			if conf.StorageAccountName == "" ||
				conf.StorageAccountKey == "" {
				errMsg = append(errMsg, "invalid Azure storage configuration")
			}
			if conf.StorageAccountName == "" && conf.StorageAccountKey != "" {
				errMsg = append(errMsg, "no Azure storage_account specified while storage_account_key is present in config file; both should be present")
			}
			if conf.StorageAccountName != "" && conf.StorageAccountKey == "" {
				errMsg = append(errMsg, "no Azure storage_account_key specified while storage_account is present in config file; both should be present")
			}
		} else {
			if conf.StorageAccountName == "" {
				errMsg = append(errMsg, "UserAssignedID is configured but storage account name is missing")
			}
			if conf.StorageAccountKey != "" {
				errMsg = append(errMsg, "UserAssignedID is configured but storage account key is used")
			}
		}
	} else {
		if conf.StorageAccountName == "" {
			errMsg = append(errMsg, "MSI resource is configured but storage account name is missing")
		}
		if conf.StorageAccountKey != "" {
			errMsg = append(errMsg, "MSI resource is configured but storage account key is used")
		}
	}

	if conf.ContainerName == "" {
		errMsg = append(errMsg, "no Azure container specified")
	}
	if conf.Endpoint == "" {
		conf.Endpoint = azureDefaultEndpoint
	}

	if conf.PipelineConfig.MaxTries < 0 {
		errMsg = append(errMsg, "The value of max_tries must be greater than or equal to 0 in the config file")
	}

	if conf.ReaderConfig.MaxRetryRequests < 0 {
		errMsg = append(errMsg, "The value of max_retry_requests must be greater than or equal to 0 in the config file")
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// parseConfig unmarshals a buffer into a Config with default values.
func parseConfig(conf []byte) (Config, error) {
	config := DefaultConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return Config{}, err
	}

	// If we don't have config specific retry values but we do have the generic MaxRetries.
	// This is for backwards compatibility but also ease of configuration.
	if config.MaxRetries > 0 {
		if config.PipelineConfig.MaxTries == 0 {
			config.PipelineConfig.MaxTries = int32(config.MaxRetries)
		}
		if config.ReaderConfig.MaxRetryRequests == 0 {
			config.ReaderConfig.MaxRetryRequests = config.MaxRetries
		}
	}

	return config, nil
}
