// Package mfs provides a mutable filesystem layer using kubo's MFS API
package mfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Client handles communication with kubo's MFS API
type Client struct {
	apiURL   string
	debug    bool
	mfsRoot  string // Root path in MFS (e.g., "/storachafs")
	mu       sync.RWMutex
	modified bool // Track if any modifications were made
}

// NewClient creates a new MFS client
func NewClient(debug bool, mfsRoot string) *Client {
	if mfsRoot == "" {
		mfsRoot = "/storachafs"
	}
	return &Client{
		apiURL:  "http://127.0.0.1:5001/api/v0",
		debug:   debug,
		mfsRoot: mfsRoot,
	}
}

// Initialize sets up the MFS root directory, optionally copying from an existing CID
func (c *Client) Initialize(ctx context.Context, baseCID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove existing MFS root if it exists
	c.filesRm(ctx, c.mfsRoot, true)

	// If we have a base CID, copy it directly as the MFS root
	// This replaces the entire root with the CID contents
	if baseCID != "" {
		if c.debug {
			log.Printf("Copying CID %s into MFS at %s", baseCID, c.mfsRoot)
		}
		// Copy the CID directly as the root - this works because we removed it first
		if err := c.filesCp(ctx, "/ipfs/"+baseCID, c.mfsRoot); err != nil {
			return fmt.Errorf("failed to copy base CID to MFS: %w", err)
		}
	} else {
		// No base CID - create an empty directory
		if c.debug {
			log.Printf("Creating empty MFS root at %s", c.mfsRoot)
		}
		if err := c.filesMkdir(ctx, c.mfsRoot, true); err != nil {
			return fmt.Errorf("failed to create MFS root: %w", err)
		}
	}

	return nil
}

// GetRootCID returns the current CID of the MFS root
func (c *Client) GetRootCID(ctx context.Context) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.filesFlush(ctx, c.mfsRoot)
}

// IsModified returns true if any modifications were made
func (c *Client) IsModified() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.modified
}

// mfsPath converts a relative path to an MFS path
func (c *Client) mfsPath(relPath string) string {
	if relPath == "" || relPath == "/" {
		return c.mfsRoot
	}
	return filepath.Join(c.mfsRoot, relPath)
}

// Stat returns information about a file or directory in MFS
type StatInfo struct {
	Hash           string `json:"Hash"`
	Size           uint64 `json:"Size"`
	CumulativeSize uint64 `json:"CumulativeSize"`
	Blocks         int    `json:"Blocks"`
	Type           string `json:"Type"` // "file" or "directory"
}

func (c *Client) Stat(ctx context.Context, path string) (*StatInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mfsPath := c.mfsPath(path)
	reqURL := fmt.Sprintf("%s/files/stat?arg=%s", c.apiURL, url.QueryEscape(mfsPath))

	if c.debug {
		log.Printf("MFS stat: %s", mfsPath)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MFS stat failed: %s", string(body))
	}

	var info StatInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}

	return &info, nil
}

// Entry represents a file or directory entry
type Entry struct {
	Name string
	Type int    // 0 = file, 1 = directory
	Size uint64
	Hash string
}

// List returns the contents of a directory in MFS
func (c *Client) List(ctx context.Context, path string) ([]Entry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.filesLs(ctx, c.mfsPath(path))
}

func (c *Client) filesLs(ctx context.Context, mfsPath string) ([]Entry, error) {
	reqURL := fmt.Sprintf("%s/files/ls?arg=%s&long=true", c.apiURL, url.QueryEscape(mfsPath))

	if c.debug {
		log.Printf("MFS ls: %s", mfsPath)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MFS ls failed: %s", string(body))
	}

	var result struct {
		Entries []struct {
			Name string `json:"Name"`
			Type int    `json:"Type"`
			Size uint64 `json:"Size"`
			Hash string `json:"Hash"`
		} `json:"Entries"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	entries := make([]Entry, len(result.Entries))
	for i, e := range result.Entries {
		entries[i] = Entry{
			Name: e.Name,
			Type: e.Type,
			Size: e.Size,
			Hash: e.Hash,
		}
	}

	return entries, nil
}

// Read reads file content from MFS
func (c *Client) Read(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mfsPath := c.mfsPath(path)
	reqURL := fmt.Sprintf("%s/files/read?arg=%s&offset=%d&count=%d", c.apiURL, url.QueryEscape(mfsPath), offset, length)

	if c.debug {
		log.Printf("MFS read: %s [%d:%d]", mfsPath, offset, length)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MFS read failed: %s", string(body))
	}

	return io.ReadAll(resp.Body)
}

// Write writes data to a file in MFS
func (c *Client) Write(ctx context.Context, path string, data []byte, offset int64, create, truncate bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mfsPath := c.mfsPath(path)

	// Build query params
	reqURL := fmt.Sprintf("%s/files/write?arg=%s&offset=%d&create=%t&truncate=%t&parents=true",
		c.apiURL, url.QueryEscape(mfsPath), offset, create, truncate)

	if c.debug {
		log.Printf("MFS write: %s [offset=%d, len=%d, create=%t, truncate=%t]",
			mfsPath, offset, len(data), create, truncate)
	}

	// Create multipart form with file data
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "data")
	if err != nil {
		return err
	}
	if _, err := part.Write(data); err != nil {
		return err
	}
	writer.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MFS write failed: %s", string(respBody))
	}

	c.modified = true
	return nil
}

// Mkdir creates a directory in MFS
func (c *Client) Mkdir(ctx context.Context, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.filesMkdir(ctx, c.mfsPath(path), true); err != nil {
		return err
	}

	c.modified = true
	return nil
}

func (c *Client) filesMkdir(ctx context.Context, mfsPath string, parents bool) error {
	reqURL := fmt.Sprintf("%s/files/mkdir?arg=%s&parents=%t", c.apiURL, url.QueryEscape(mfsPath), parents)

	if c.debug {
		log.Printf("MFS mkdir: %s", mfsPath)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MFS mkdir failed: %s", string(body))
	}

	return nil
}

// Remove removes a file or directory from MFS
func (c *Client) Remove(ctx context.Context, path string, recursive bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.filesRm(ctx, c.mfsPath(path), recursive); err != nil {
		return err
	}

	c.modified = true
	return nil
}

func (c *Client) filesRm(ctx context.Context, mfsPath string, recursive bool) error {
	reqURL := fmt.Sprintf("%s/files/rm?arg=%s&recursive=%t&force=true", c.apiURL, url.QueryEscape(mfsPath), recursive)

	if c.debug {
		log.Printf("MFS rm: %s (recursive=%t)", mfsPath, recursive)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	// Ignore 500 errors for rm (file might not exist)
	if resp.StatusCode != 200 && resp.StatusCode != 500 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MFS rm failed: %s", string(body))
	}

	return nil
}

// Rename moves/renames a file or directory in MFS
func (c *Client) Rename(ctx context.Context, oldPath, newPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldMfsPath := c.mfsPath(oldPath)
	newMfsPath := c.mfsPath(newPath)

	reqURL := fmt.Sprintf("%s/files/mv?arg=%s&arg=%s", c.apiURL, url.QueryEscape(oldMfsPath), url.QueryEscape(newMfsPath))

	if c.debug {
		log.Printf("MFS mv: %s -> %s", oldMfsPath, newMfsPath)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MFS mv failed: %s", string(body))
	}

	c.modified = true
	return nil
}

// Truncate changes the size of a file
func (c *Client) Truncate(ctx context.Context, path string, size uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mfsPath := c.mfsPath(path)

	// Read current content
	url := fmt.Sprintf("%s/files/read?arg=%s", c.apiURL, mfsPath)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}

	var data []byte
	if resp.StatusCode == 200 {
		data, _ = io.ReadAll(resp.Body)
	}
	resp.Body.Close()

	// Truncate or extend
	if uint64(len(data)) > size {
		data = data[:size]
	} else if uint64(len(data)) < size {
		newData := make([]byte, size)
		copy(newData, data)
		data = newData
	}

	// Write back
	writeURL := fmt.Sprintf("%s/files/write?arg=%s&offset=0&create=true&truncate=true", c.apiURL, mfsPath)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "data")
	if err != nil {
		return err
	}
	if _, err := part.Write(data); err != nil {
		return err
	}
	writer.Close()

	req, err = http.NewRequestWithContext(ctx, "POST", writeURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MFS truncate failed: %s", string(respBody))
	}

	c.modified = true
	return nil
}

func (c *Client) filesCp(ctx context.Context, src, dst string) error {
	reqURL := fmt.Sprintf("%s/files/cp?arg=%s&arg=%s", c.apiURL, url.QueryEscape(src), url.QueryEscape(dst))

	if c.debug {
		log.Printf("MFS cp: %s -> %s", src, dst)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("MFS cp failed: %s", string(body))
	}

	return nil
}

// Flush ensures all changes are written and returns the CID
func (c *Client) filesFlush(ctx context.Context, mfsPath string) (string, error) {
	reqURL := fmt.Sprintf("%s/files/stat?arg=%s&hash=true", c.apiURL, url.QueryEscape(mfsPath))

	if c.debug {
		log.Printf("MFS flush/stat: %s", mfsPath)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("MFS flush failed: %s", string(body))
	}

	var result struct {
		Hash string `json:"Hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Hash, nil
}

// ExportCAR exports the MFS root as a CAR file for upload to Storacha
func (c *Client) ExportCAR(ctx context.Context, outputPath string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First get the root CID
	rootCID, err := c.filesFlush(ctx, c.mfsRoot)
	if err != nil {
		return "", fmt.Errorf("failed to get root CID: %w", err)
	}

	// Export as CAR
	reqURL := fmt.Sprintf("%s/dag/export?arg=%s", c.apiURL, url.QueryEscape(rootCID))

	if c.debug {
		log.Printf("Exporting CAR for CID %s to %s", rootCID, outputPath)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("MFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("CAR export failed: %s", string(body))
	}

	// Write to file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("failed to create CAR file: %w", err)
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, resp.Body); err != nil {
		return "", fmt.Errorf("failed to write CAR file: %w", err)
	}

	return rootCID, nil
}

// Cleanup removes the MFS working directory
func (c *Client) Cleanup(ctx context.Context) error {
	return c.filesRm(ctx, c.mfsRoot, true)
}

// FileInfo represents file metadata for FUSE
type FileInfo struct {
	Name    string
	IsDir   bool
	Size    uint64
	ModTime time.Time
	Hash    string
}

// GetFileInfo returns file info for FUSE operations
func (c *Client) GetFileInfo(ctx context.Context, path string) (*FileInfo, error) {
	stat, err := c.Stat(ctx, path)
	if err != nil {
		return nil, err
	}

	_, name := filepath.Split(path)

	return &FileInfo{
		Name:    name,
		IsDir:   stat.Type == "directory",
		Size:    stat.Size,
		ModTime: time.Now(), // MFS doesn't track modification time
		Hash:    stat.Hash,
	}, nil
}
