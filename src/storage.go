package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
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

// bytesToETag generates an MD5-based ETag from byte data
func bytesToETag(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

// StorageConfig interface defines root directory access
type StorageConfig interface {
	RootDir() string
}

// S3Config holds configuration for S3 storage
type S3Config struct {
	rootDir         string
	AccessKey       string
	SecretKey       string
	Endpoint        string
	Bucket          string
	UsePathStyle    bool
	Region          string
	PublicURLPrefix string
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
		rootDir:         rootDir,
		AccessKey:       os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey:       os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Endpoint:        os.Getenv("S3_ENDPOINT"),
		Bucket:          os.Getenv("S3_BUCKET"),
		UsePathStyle:    os.Getenv("S3_USE_PATH_STYLE") == "true",
		Region:          region,
		PublicURLPrefix: os.Getenv("S3_PUBLIC_URL_PREFIX"),
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

// WriteOption configures write operations
type WriteOption func(*writeConfig)

type writeConfig struct {
	ifMatch string
}

func WithIfMatch(etag string) WriteOption {
	return func(c *writeConfig) {
		c.ifMatch = etag
	}
}

// Storage interface replaces OpenDAL operations
type Storage interface {
	Read(path string) ([]byte, *s3FileInfo, error)
	Write(path string, data []byte, opts ...WriteOption) error
	CreateDir(path string) error
	Stat(path string) (*s3FileInfo, error)
	Delete(path string) error
	ToDuckDBWritePath(path string) string
	ToDuckDBReadPath(path string) string
	List(prefix string) ([]string, error)
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

func (s *S3Storage) List(prefix string) ([]string, error) {
	fullPrefix := s.fullKey(prefix)
	var objects []string

	s.logger.Printf("Listing objects in bucket=%s prefix=%s", s.config.Bucket, fullPrefix)

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(fullPrefix),
	})

	// Trim the root directory prefix from returned keys
	rootPrefix := s.fullKey("")
	trimLength := len(rootPrefix)
	if trimLength > 0 && !strings.HasSuffix(rootPrefix, "/") {
		trimLength++ // Also trim the trailing slash if present
	}

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			trimmedKey := (*obj.Key)[trimLength:]
			objects = append(objects, trimmedKey)
		}
	}
	return objects, nil
}

func (s *S3Storage) Read(path string) ([]byte, *s3FileInfo, error) {
	fullKey := s.fullKey(path)
	var fileInfo *s3FileInfo
	var err error

	defer func() {
		status := "success"
		if err != nil {
			status = fmt.Sprintf("error: %v", err)
		}
		if fileInfo != nil {
			s.logger.Printf("S3 Read operation: bucket=%s key=%s size=%d etag=%s mod_time=%s status=%s",
				s.config.Bucket, fullKey, fileInfo.size, fileInfo.etag, fileInfo.modTime.Format(time.RFC3339), status)
		} else {
			s.logger.Printf("S3 Read operation: bucket=%s key=%s status=%s",
				s.config.Bucket, fullKey, status)
		}
	}()

	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	// Build file info from response headers
	fileInfo = &s3FileInfo{
		name:  filepath.Base(path),
		isDir: strings.HasSuffix(path, "/"),
	}

	if resp.ContentLength != nil {
		fileInfo.size = *resp.ContentLength
	}
	if resp.LastModified != nil {
		fileInfo.modTime = *resp.LastModified
	}
	if resp.ETag != nil {
		fileInfo.etag = strings.Trim(*resp.ETag, `"`)
	}

	data, err := io.ReadAll(resp.Body)
	return data, fileInfo, err
}

func (s *S3Storage) Write(path string, data []byte, opts ...WriteOption) error {
	fullKey := s.fullKey(path)
	var cfg writeConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var etag string
	defer func() {
		etagClean := strings.Trim(etag, `"`)
		s.logger.Printf("Writing object to S3: bucket=%s key=%s size=%d etag=%s cfg=%+v",
			s.config.Bucket, fullKey, len(data), etagClean, cfg)
	}()

	putInput := &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
		Body:   bytes.NewReader(data),
	}

	if cfg.ifMatch != "" {
		putInput.IfMatch = aws.String(cfg.ifMatch)
		s.logger.Printf("Conditional write with IfMatch: %s", cfg.ifMatch)
	}

	resp, err := s.client.PutObject(context.Background(), putInput)
	if err != nil {
		s.logger.Printf("Error writing object: %v", err)
		return err
	}

	// Capture ETag from response
	if resp.ETag != nil {
		etag = *resp.ETag
	}

	return nil
}

func (s *S3Storage) CreateDir(path string) error {
	// No-op for S3 since directories are implicit in object keys
	s.logger.Printf("Skipping directory creation for S3: bucket=%s path=%s",
		s.config.Bucket, path)
	return nil
}

func (s *S3Storage) Stat(path string) (*s3FileInfo, error) {
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

	// Capture ETag (MD5 for single-part uploads)
	etag := ""
	if resp.ETag != nil {
		etag = strings.Trim(*resp.ETag, `"`)
	}

	return &s3FileInfo{
		name:    filepath.Base(path),
		size:    size,
		modTime: aws.ToTime(resp.LastModified),
		etag:    etag,
		isDir:   strings.HasSuffix(path, "/"), // Detect directory markers
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

func (s *S3Storage) ToDuckDBWritePath(path string) string {
	ret := "s3://" + filepath.Join(s.config.Bucket, s.config.rootDir, path)
	fmt.Printf("ToDuckDBWritePath: %s\n", ret)
	return ret
}

func (s *S3Storage) ToDuckDBReadPath(path string) string {
	if s.config.PublicURLPrefix != "" {
		ret := s.config.PublicURLPrefix + "/" + filepath.Join(s.config.rootDir, path)
		fmt.Printf("ToDuckDBReadPath: %s\n", ret)
		return ret
	}
	return s.ToDuckDBWritePath(path)
}

func (s *S3Storage) ToDuckDBSecret(secretName string) string {
	if s.config.AccessKey == "" || s.config.SecretKey == "" {
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
	etag    string
	isDir   bool
}

func (fi *s3FileInfo) ETag() string { return fi.etag }

func (fi *s3FileInfo) Name() string       { return fi.name }
func (fi *s3FileInfo) Size() int64        { return fi.size }
func (fi *s3FileInfo) Mode() os.FileMode  { return 0644 }
func (fi *s3FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *s3FileInfo) IsDir() bool        { return fi.isDir }
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

func (fs *FSStorage) Read(path string) ([]byte, *s3FileInfo, error) {
	fullPath := fs.fullPath(path)

	file, err := os.Open(fullPath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}

	etagChecksum := bytesToETag(data)

	return data, &s3FileInfo{
		name:    fi.Name(),
		size:    fi.Size(),
		modTime: fi.ModTime(),
		etag:    etagChecksum,
		isDir:   fi.IsDir(),
	}, nil
}

func (fs *FSStorage) Write(path string, data []byte, opts ...WriteOption) error {
	fullPath := fs.fullPath(path)
	var cfg writeConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.ifMatch != "" {
		fi, err := fs.Stat(path)
		if err != nil {
			// complain that path does not exist
			return fmt.Errorf("failed to check etag, %s does not exist: %w", path, err)
		}
		if fi.ETag() != cfg.ifMatch {
			return fmt.Errorf("IfMatch: ETag mismatch (current: %s)", fi.ETag())
		}
		log.Printf("FS.Write(ETag=%s) etag as expected in pre-existing file %s)",
			fi.ETag(), fullPath)
	}

	err := os.WriteFile(fullPath, data, 0644)
	if err != nil && os.IsNotExist(err) {
		// Only check/create directory if initial write failed
		dir := filepath.Dir(fullPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
		return os.WriteFile(fullPath, data, 0644)
	}
	return err
}

func (fs *FSStorage) CreateDir(path string) error {
	return os.MkdirAll(fs.fullPath(path), 0755)
}

func (fs *FSStorage) Stat(path string) (*s3FileInfo, error) {
	fullPath := fs.fullPath(path)
	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}

	var etagChecksum string
	if !fi.IsDir() {
		data, err := os.ReadFile(fullPath) // Read file once
		if err != nil {
			return nil, err
		}
		etagChecksum = bytesToETag(data) // Use shared helper
	}

	return &s3FileInfo{
		name:    fi.Name(),
		size:    fi.Size(),
		modTime: fi.ModTime(),
		etag:    etagChecksum,
		isDir:   fi.IsDir(),
	}, nil
}

func (fs *FSStorage) Delete(path string) error {
	return os.Remove(fs.fullPath(path))
}

func (fs *FSStorage) ToDuckDBWritePath(path string) string {
	return filepath.Join(fs.config.rootDir, path)
}

func (fs *FSStorage) ToDuckDBReadPath(path string) string {
	return fs.ToDuckDBWritePath(path)
}

func (fs *FSStorage) List(prefix string) ([]string, error) {
	fullPrefix := fs.fullPath(prefix)
	var files []string

	err := filepath.WalkDir(fullPrefix, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			log.Printf("FSStorage.List: Error accessing path %q: %v", path, err)
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		relPath, err := filepath.Rel(fs.config.rootDir, path)
		if err != nil {
			log.Printf("FSStorage.List: Error converting path %q to relative: %v", path, err)
			return err
		}
		if d.IsDir() || relPath == "." {
			return nil
		}
		files = append(files, relPath)
		return nil
	})

	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
	}

	return files, err
}

func (fs *FSStorage) ToDuckDBSecret(secretName string) string {
	return "" // No secret for filesystem storage
}
