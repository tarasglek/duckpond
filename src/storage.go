package main

import (
	"os"
	"path/filepath"
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
