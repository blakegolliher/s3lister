package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

type S3Config struct {
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Endpoint  string `toml:"endpoint"`
	Bucket    string `toml:"bucket"`
	Prefix    string `toml:"prefix"`
	Region    string `toml:"region"`
}

type WorkersConfig struct {
	Readers   int `toml:"readers"`
	Writers   int `toml:"writers"`
	QueueSize int `toml:"queue_size"`
}

type StorageConfig struct {
	DBPath string `toml:"db_path"`
}

type LoggingConfig struct {
	LogFile string `toml:"log_file"`
}

type Config struct {
	S3      S3Config      `toml:"s3"`
	Workers WorkersConfig `toml:"workers"`
	Storage StorageConfig `toml:"storage"`
	Logging LoggingConfig `toml:"logging"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if c.S3.AccessKey == "" || c.S3.AccessKey == "YOUR_ACCESS_KEY" {
		return fmt.Errorf("s3.access_key is required")
	}
	if c.S3.SecretKey == "" || c.S3.SecretKey == "YOUR_SECRET_KEY" {
		return fmt.Errorf("s3.secret_key is required")
	}
	if c.S3.Endpoint == "" {
		return fmt.Errorf("s3.endpoint is required")
	}
	if c.S3.Bucket == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	if c.S3.Region == "" {
		c.S3.Region = "us-east-1"
	}

	c.Workers.Readers = clampDefault(c.Workers.Readers, 1, 512, 32)
	c.Workers.Writers = clampDefault(c.Workers.Writers, 1, 128, 8)
	c.Workers.QueueSize = clampDefault(c.Workers.QueueSize, 1000, 10_000_000, 100_000)

	if c.Storage.DBPath == "" {
		c.Storage.DBPath = "./s3lister.db"
	}
	if c.Logging.LogFile == "" {
		c.Logging.LogFile = "./s3lister.log"
	}
	return nil
}

// clampDefault returns val clamped to [min, max], or def if val <= 0.
func clampDefault(val, min, max, def int) int {
	if val <= 0 {
		return def
	}
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}
