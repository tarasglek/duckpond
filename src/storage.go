package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// StorageConfig interface defines root directory access
type StorageConfig interface {
	RootDir() string
}

// S3Config holds configuration for S3 storage
type S3Config struct {
	rootDir      string
	AccessKey    string
	SecretKey    string
	Endpoint     string
	Bucket       string
	UsePathStyle bool
	Region       string
}

func (c *S3Config) RootDir() string {
	return c.rootDir
}

func LoadS3ConfigFromEnv(rootDir string) *S3Config {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1" // Default region
	}
	return &S3Config{
		rootDir:      rootDir,
		AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Endpoint:     os.Getenv("S3_ENDPOINT"),
		Bucket:       os.Getenv("S3_BUCKET"),
		UsePathStyle: os.Getenv("S3_USE_PATH_STYLE") == "true",
		Region:       region,
	}
}

func (c *S3Config) LoadAWSConfig() (aws.Config, error) {
	return config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     c.AccessKey,
				SecretAccessKey: c.SecretKey,
			}, nil
		})),
		config.WithRegion(c.Region),
	)
}

// Storage interface replaces OpenDAL operations
type Storage interface {
	Read(path string) ([]byte, error)
	Write(path string, data []byte) error
	CreateDir(path string) error
	Stat(path string) (os.FileInfo, error)
	Delete(path string) error
	ToDuckDBPath(path string) string
	ToDuckDBSecret(secretName string) string
}

// FSConfig holds configuration for local filesystem storage
type FSConfig struct {
	rootDir string
}

func (c *FSConfig) RootDir() string {
	return c.rootDir
}

// FSStorage implements Storage using local filesystem
type FSStorage struct {
	config *FSConfig
}

func NewFSStorage(config *FSConfig) Storage {
	return &FSStorage{config: config}
}

// S3Storage implements Storage using S3/MinIO
type S3Storage struct {
	client *s3.Client
	config *S3Config
	logger *log.Logger
}

func NewS3Storage(config *S3Config) Storage {
	cfg, err := config.LoadAWSConfig()
	if err != nil {
		panic("failed to load AWS config: " + err.Error())
	}

	return &S3Storage{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = config.UsePathStyle
			if config.Endpoint != "" {
				o.BaseEndpoint = &config.Endpoint
			}
		}),
		config: config,
		logger: log.New(os.Stdout, "[S3Storage] ", log.LstdFlags|log.Lshortfile),
	}
}

func (s *S3Storage) fullKey(path string) string {
	return strings.TrimPrefix(filepath.Join(s.config.RootDir(), path), "/")
}

func (s *S3Storage) Read(path string) ([]byte, error) {
	fullKey := s.fullKey(path)
	s.logger.Printf("Reading object from S3: bucket=%s key=%s", s.config.Bucket, fullKey)

	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		s.logger.Printf("Error reading object: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logger.Printf("Error reading object body: %v", err)
	}
	return data, err
}

func (s *S3Storage) Write(path string, data []byte) error {
	fullKey := s.fullKey(path)
	s.logger.Printf("Writing object to S3: bucket=%s key=%s size=%d",
		s.config.Bucket, fullKey, len(data))

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		s.logger.Printf("Error writing object: %v", err)
	}
	return err
}

func (s *S3Storage) CreateDir(path string) error {
	key := s.fullKey(path) + "/"
	s.logger.Printf("Creating directory marker: bucket=%s key=%s",
		s.config.Bucket, key)

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}),
	})
	if err != nil {
		s.logger.Printf("Error creating directory marker: %v", err)
	}
	return err
}

func (s *S3Storage) Stat(path string) (os.FileInfo, error) {
	fullKey := s.fullKey(path)
	s.logger.Printf("Getting object metadata: bucket=%s key=%s",
		s.config.Bucket, fullKey)

	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		s.logger.Printf("Error getting object metadata: %v", err)
		return nil, err
	}

	// Handle nil pointer for ContentLength
	size := int64(0)
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}

	return &s3FileInfo{
		name:    filepath.Base(path),
		size:    size,
		modTime: aws.ToTime(resp.LastModified),
	}, nil
}

func (s *S3Storage) Delete(path string) error {
	fullKey := s.fullKey(path)
	s.logger.Printf("Deleting object: bucket=%s key=%s",
		s.config.Bucket, fullKey)

	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		s.logger.Printf("Error deleting object: %v", err)
	}
	return err
}

func (s *S3Storage) ToDuckDBPath(path string) string {
	return "s3://" + filepath.Join(s.config.Bucket, s.config.rootDir, path)
}

func (s *S3Storage) ToDuckDBSecret(secretName string) string {
	s.logger.Printf("Generating DuckDB secret for S3 access")

	if s.config.AccessKey == "" || s.config.SecretKey == "" {
		s.logger.Printf("No access key or secret key configured, returning empty secret")
		return ""
	}
	parts := []string{
		"TYPE S3",
		fmt.Sprintf("KEY_ID '%s'", s.config.AccessKey),
		fmt.Sprintf("SECRET '%s'", s.config.SecretKey),
		fmt.Sprintf("REGION '%s'", s.config.Region),
	}

	if s.config.Endpoint != "" {
		// Parse endpoint to extract host:port without protocol
		endpointURL, err := url.Parse(s.config.Endpoint)
		if err != nil {
			s.logger.Printf("Invalid endpoint URL: %v", err)
			return ""
		}

		parts = append(parts,
			fmt.Sprintf("ENDPOINT '%s'", endpointURL.Host),
			fmt.Sprintf("USE_SSL '%t'", endpointURL.Scheme == "https"),
		)
	}

	// Add path style configuration separately from endpoint
	if s.config.UsePathStyle {
		parts = append(parts, "URL_STYLE 'path'")
	}

	secret := fmt.Sprintf(
		"CREATE OR REPLACE SECRET %s (\n    %s\n);",
		secretName,
		strings.Join(parts, ",\n    "),
	)

	// Create redacted version for logging
	redactedParts := make([]string, len(parts))
	copy(redactedParts, parts)
	for i, p := range redactedParts {
		if strings.HasPrefix(p, "KEY_ID") {
			redactedParts[i] = "KEY_ID '[REDACTED]'"
		}
		if strings.HasPrefix(p, "SECRET") {
			redactedParts[i] = "SECRET '[REDACTED]'"
		}
	}

	redactedSecret := fmt.Sprintf(
		"CREATE OR REPLACE SECRET %s (\n    %s\n);",
		secretName,
		strings.Join(redactedParts, ",\n    "),
	)

	s.logger.Printf("Generated DuckDB secret (redacted):\n%s", redactedSecret)
	return secret
}

// Helper struct to implement os.FileInfo for S3
type s3FileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi *s3FileInfo) Name() string       { return fi.name }
func (fi *s3FileInfo) Size() int64        { return fi.size }
func (fi *s3FileInfo) Mode() os.FileMode  { return 0644 }
func (fi *s3FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *s3FileInfo) IsDir() bool        { return false }
func (fi *s3FileInfo) Sys() interface{}   { return nil }

// NewStorage creates either S3 or FS storage based on environment
func NewStorage(rootDir string) Storage {
	s3Config := LoadS3ConfigFromEnv(rootDir)
	if s3Config.Bucket != "" {
		return NewS3Storage(s3Config)
	}
	return NewFSStorage(&FSConfig{rootDir: rootDir})
}

func (fs *FSStorage) fullPath(path string) string {
	return filepath.Join(fs.config.RootDir(), path)
}

func (fs *FSStorage) Read(path string) ([]byte, error) {
	return os.ReadFile(fs.fullPath(path))
}

func (fs *FSStorage) Write(path string, data []byte) error {
	return os.WriteFile(fs.fullPath(path), data, 0644)
}

func (fs *FSStorage) CreateDir(path string) error {
	return os.MkdirAll(fs.fullPath(path), 0755)
}

func (fs *FSStorage) Stat(path string) (os.FileInfo, error) {
	return os.Stat(fs.fullPath(path))
}

func (fs *FSStorage) Delete(path string) error {
	return os.Remove(fs.fullPath(path))
}

func (fs *FSStorage) ToDuckDBPath(path string) string {
	return filepath.Join(fs.config.rootDir, path)
}

func (fs *FSStorage) ToDuckDBSecret(secretName string) string {
	return "" // No secret for filesystem storage
}
