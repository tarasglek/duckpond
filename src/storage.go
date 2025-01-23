package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config holds configuration for S3 storage
type S3Config struct {
	AccessKey    string
	SecretKey    string
	Endpoint     string
	Bucket       string
	UsePathStyle bool
	Region       string
}

func LoadS3ConfigFromEnv() *S3Config {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1" // Default region
	}
	return &S3Config{
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
}

// FSStorage implements Storage using local filesystem
type FSStorage struct {
	rootDir string
}

func NewFSStorage(rootDir string) Storage {
	return &FSStorage{rootDir: rootDir}
}

// S3Storage implements Storage using S3/MinIO
type S3Storage struct {
	client   *s3.Client
	config   *S3Config
	rootDir  string
}

func NewS3Storage(config *S3Config, rootDir string) Storage {
	cfg, err := config.LoadAWSConfig()
	if err != nil {
		panic("failed to load AWS config: " + err.Error())
	}

	return &S3Storage{
		client:   s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = config.UsePathStyle
			if config.Endpoint != "" {
				o.BaseEndpoint = &config.Endpoint
			}
		}),
		config:   config,
		rootDir:  strings.TrimPrefix(rootDir, "/"),
	}
}

func (s *S3Storage) fullKey(path string) string {
	return strings.TrimPrefix(filepath.Join(s.rootDir, path), "/")
}

func (s *S3Storage) Read(path string) ([]byte, error) {
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (s *S3Storage) Write(path string, data []byte) error {
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(s.fullKey(path)),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *S3Storage) CreateDir(path string) error {
	// S3 doesn't have directories - create empty "folder" marker
	key := s.fullKey(path) + "/"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}),
	})
	return err
}

func (s *S3Storage) Stat(path string) (os.FileInfo, error) {
	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	if err != nil {
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
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	return err
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
	s3Config := LoadS3ConfigFromEnv()
	if s3Config.Bucket != "" {
		return NewS3Storage(s3Config, rootDir)
	}
	return NewFSStorage(rootDir)
}

func (fs *FSStorage) fullPath(path string) string {
	return filepath.Join(fs.rootDir, path)
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
