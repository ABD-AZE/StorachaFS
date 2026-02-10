// fuse implementation for StorachaFS
package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// IPFSClient handles communication with local IPFS daemon (kubo)
type IPFSClient struct {
	apiURL string
	debug  bool
}

// UnixFSEntry represents a file or directory in IPFS UnixFS
type UnixFSEntry struct {
	Name string
	Hash string
	Size uint64
	Type int // 1 = directory, 2 = file
}

// LSResponse represents the response from IPFS ls command
type LSResponse struct {
	Objects []struct {
		Hash  string `json:"Hash"`
		Links []struct {
			Name   string `json:"Name"`
			Hash   string `json:"Hash"`
			Size   uint64 `json:"Size"`
			Type   int    `json:"Type"`
			Target string `json:"Target"`
		} `json:"Links"`
	} `json:"Objects"`
}

// NewIPFSClient creates a new client for the local IPFS daemon
func NewIPFSClient(debug bool) *IPFSClient {
	return &IPFSClient{
		apiURL: "http://127.0.0.1:5001/api/v0",
		debug:  debug,
	}
}

// List returns the contents of a directory by CID
func (c *IPFSClient) List(ctx context.Context, cid string) ([]UnixFSEntry, error) {
	url := fmt.Sprintf("%s/ls?arg=%s", c.apiURL, cid)
	if c.debug {
		log.Printf("IPFS ls: %s", url)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("IPFS API error (is ipfs daemon running?): %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("IPFS ls failed: %s", string(body))
	}

	var lsResp LSResponse
	if err := json.NewDecoder(resp.Body).Decode(&lsResp); err != nil {
		return nil, fmt.Errorf("failed to decode IPFS ls response: %w", err)
	}

	if len(lsResp.Objects) == 0 {
		return nil, nil
	}

	var entries []UnixFSEntry
	for _, link := range lsResp.Objects[0].Links {
		entries = append(entries, UnixFSEntry{
			Name: link.Name,
			Hash: link.Hash,
			Size: link.Size,
			Type: link.Type,
		})
	}
	return entries, nil
}

// Cat returns a reader for file content by CID
func (c *IPFSClient) Cat(ctx context.Context, cid string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/cat?arg=%s", c.apiURL, cid)
	if c.debug {
		log.Printf("IPFS cat: %s", url)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("IPFS API error: %w", err)
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("IPFS cat failed: %s", string(body))
	}

	return resp.Body, nil
}

// CatRange returns a portion of file content by CID with offset and length
func (c *IPFSClient) CatRange(ctx context.Context, cid string, offset, length int64) ([]byte, error) {
	url := fmt.Sprintf("%s/cat?arg=%s&offset=%d&length=%d", c.apiURL, cid, offset, length)
	if c.debug {
		log.Printf("IPFS cat range: %s", url)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("IPFS API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("IPFS cat failed: %s", string(body))
	}

	return io.ReadAll(resp.Body)
}

// Stat returns information about a CID (whether it's a file or directory)
func (c *IPFSClient) Stat(ctx context.Context, cid string) (isDir bool, size uint64, err error) {
	url := fmt.Sprintf("%s/files/stat?arg=/ipfs/%s", c.apiURL, cid)
	if c.debug {
		log.Printf("IPFS stat: %s", url)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return false, 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Fallback: try ls to determine if it's a directory
		entries, err := c.List(ctx, cid)
		if err == nil && len(entries) > 0 {
			return true, 0, nil
		}
		// If ls returns empty or fails, assume it's a file
		return false, 0, nil
	}

	var statResp struct {
		Type           string `json:"Type"`
		Size           uint64 `json:"Size"`
		CumulativeSize uint64 `json:"CumulativeSize"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&statResp); err != nil {
		return false, 0, err
	}

	return statResp.Type == "directory", statResp.Size, nil
}

// ------------------- FUSE Filesystem Implementation -------------------

// StorachaFS is the root of the FUSE filesystem
type StorachaFS struct {
	fs.Inode
	cid    string
	client *IPFSClient
	debug  bool
}

// NewStorachaFS creates a new FUSE filesystem rooted at the given CID
func NewStorachaFS(rootCID string, debug bool) *StorachaFS {
	return &StorachaFS{
		cid:    rootCID,
		client: NewIPFSClient(debug),
		debug:  debug,
	}
}

var _ = (fs.NodeReaddirer)((*StorachaFS)(nil))
var _ = (fs.NodeLookuper)((*StorachaFS)(nil))
var _ = (fs.NodeGetattrer)((*StorachaFS)(nil))

func (r *StorachaFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0555
	out.Atime = uint64(time.Now().Unix())
	out.Mtime = out.Atime
	out.Ctime = out.Atime
	return 0
}

func (r *StorachaFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := r.client.List(ctx, r.cid)
	if err != nil {
		if r.debug {
			log.Printf("Readdir error for %s: %v", r.cid, err)
		}
		return nil, syscall.EIO
	}

	var dirEntries []fuse.DirEntry
	for _, e := range entries {
		mode := uint32(fuse.S_IFREG)
		if e.Type == 1 {
			mode = fuse.S_IFDIR
		}
		dirEntries = append(dirEntries, fuse.DirEntry{
			Mode: mode,
			Name: e.Name,
		})
	}
	return fs.NewListDirStream(dirEntries), 0
}

func (r *StorachaFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	entries, err := r.client.List(ctx, r.cid)
	if err != nil {
		if r.debug {
			log.Printf("Lookup error for %s/%s: %v", r.cid, name, err)
		}
		return nil, syscall.EIO
	}

	for _, e := range entries {
		if e.Name == name {
			if e.Type == 1 {
				// Directory
				out.Mode = fuse.S_IFDIR | 0555
				child := &StorachaDir{
					cid:    e.Hash,
					client: r.client,
					debug:  r.debug,
				}
				return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
			} else {
				// File
				out.Mode = fuse.S_IFREG | 0444
				out.Size = e.Size
				child := &StorachaFile{
					cid:    e.Hash,
					size:   e.Size,
					client: r.client,
					debug:  r.debug,
				}
				return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
			}
		}
	}
	return nil, syscall.ENOENT
}

// StorachaDir represents a directory in the FUSE filesystem
type StorachaDir struct {
	fs.Inode
	cid    string
	client *IPFSClient
	debug  bool
}

var _ = (fs.NodeReaddirer)((*StorachaDir)(nil))
var _ = (fs.NodeLookuper)((*StorachaDir)(nil))
var _ = (fs.NodeGetattrer)((*StorachaDir)(nil))

func (d *StorachaDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0555
	out.Atime = uint64(time.Now().Unix())
	out.Mtime = out.Atime
	out.Ctime = out.Atime
	return 0
}

func (d *StorachaDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := d.client.List(ctx, d.cid)
	if err != nil {
		if d.debug {
			log.Printf("Readdir error for %s: %v", d.cid, err)
		}
		return nil, syscall.EIO
	}

	var dirEntries []fuse.DirEntry
	for _, e := range entries {
		mode := uint32(fuse.S_IFREG)
		if e.Type == 1 {
			mode = fuse.S_IFDIR
		}
		dirEntries = append(dirEntries, fuse.DirEntry{
			Mode: mode,
			Name: e.Name,
		})
	}
	return fs.NewListDirStream(dirEntries), 0
}

func (d *StorachaDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	entries, err := d.client.List(ctx, d.cid)
	if err != nil {
		if d.debug {
			log.Printf("Lookup error for %s/%s: %v", d.cid, name, err)
		}
		return nil, syscall.EIO
	}

	for _, e := range entries {
		if e.Name == name {
			if e.Type == 1 {
				out.Mode = fuse.S_IFDIR | 0555
				child := &StorachaDir{
					cid:    e.Hash,
					client: d.client,
					debug:  d.debug,
				}
				return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
			} else {
				out.Mode = fuse.S_IFREG | 0444
				out.Size = e.Size
				child := &StorachaFile{
					cid:    e.Hash,
					size:   e.Size,
					client: d.client,
					debug:  d.debug,
				}
				return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
			}
		}
	}
	return nil, syscall.ENOENT
}

// StorachaFile represents a file in the FUSE filesystem
type StorachaFile struct {
	fs.Inode
	cid    string
	size   uint64
	client *IPFSClient
	debug  bool
}

var _ = (fs.NodeGetattrer)((*StorachaFile)(nil))
var _ = (fs.NodeOpener)((*StorachaFile)(nil))
var _ = (fs.NodeReader)((*StorachaFile)(nil))

func (f *StorachaFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = f.size
	out.Atime = uint64(time.Now().Unix())
	out.Mtime = out.Atime
	out.Ctime = out.Atime
	return 0
}

func (f *StorachaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Return a streaming file handle - no data is fetched here
	return &streamingFileHandle{
		cid:    f.cid,
		size:   f.size,
		client: f.client,
		debug:  f.debug,
	}, fuse.FOPEN_KEEP_CACHE, 0
}

// Read implements direct read on the inode (used by kernel for read requests)
func (f *StorachaFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(f.size) {
		return fuse.ReadResultData(nil), 0
	}

	// Calculate how much to read
	length := int64(len(dest))
	if off+length > int64(f.size) {
		length = int64(f.size) - off
	}

	// Fetch only the requested range from IPFS
	data, err := f.client.CatRange(ctx, f.cid, off, length)
	if err != nil {
		if f.debug {
			log.Printf("Read error for %s at offset %d: %v", f.cid, off, err)
		}
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data), 0
}

// streamingFileHandle implements streaming reads without loading the entire file
type streamingFileHandle struct {
	cid    string
	size   uint64
	client *IPFSClient
	debug  bool
}

var _ = (fs.FileReader)((*streamingFileHandle)(nil))

func (h *streamingFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(h.size) {
		return fuse.ReadResultData(nil), 0
	}

	// Calculate how much to read
	length := int64(len(dest))
	if off+length > int64(h.size) {
		length = int64(h.size) - off
	}

	// Fetch only the requested range from IPFS
	data, err := h.client.CatRange(ctx, h.cid, off, length)
	if err != nil {
		if h.debug {
			log.Printf("Read error for %s at offset %d: %v", h.cid, off, err)
		}
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data), 0
}