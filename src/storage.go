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
	bucket   string
	rootDir  string
}

func NewS3Storage(rootDir string) Storage {
	// Load configuration from environment variables
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
					SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
				}, nil
			}),
		),
	)
	if err != nil {
		panic("failed to load AWS config: " + err.Error())
	}

	// Configure MinIO endpoint if specified
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint != "" {
		cfg.BaseEndpoint = &endpoint
	}

	return &S3Storage{
		client:   s3.NewFromConfig(cfg, func(o *s3.Options) {
			// Enable path-style addressing for MinIO
			o.UsePathStyle = os.Getenv("S3_USE_PATH_STYLE") == "true"
		}),
		bucket:  os.Getenv("S3_BUCKET"),
		rootDir: strings.TrimPrefix(rootDir, "/"),
	}
}

func (s *S3Storage) fullKey(path string) string {
	return strings.TrimPrefix(filepath.Join(s.rootDir, path), "/")
}

func (s *S3Storage) Read(path string) ([]byte, error) {
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
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
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *S3Storage) CreateDir(path string) error {
	// S3 doesn't have directories - create empty "folder" marker
	key := s.fullKey(path) + "/"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}),
	})
	return err
}

func (s *S3Storage) Stat(path string) (os.FileInfo, error) {
	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	if err != nil {
		return nil, err
	}

	return &s3FileInfo{
		name:    filepath.Base(path),
		size:    resp.ContentLength,
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
	// Use S3 if bucket is specified in environment
	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		return NewS3Storage(rootDir)
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
