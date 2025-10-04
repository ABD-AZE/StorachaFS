package fuse

import (
	"context"
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// WriteBuffer manages in-memory writes before committing to IPFS
type WriteBuffer struct {
	data     []byte
	size     uint64
	modified bool
	mu       sync.RWMutex
}

func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{
		data: make([]byte, 0),
	}
}

func (wb *WriteBuffer) Write(data []byte, offset int64) (int, error) {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// Extend buffer if needed
	newSize := int64(len(data)) + offset
	if newSize > int64(len(wb.data)) {
		newData := make([]byte, newSize)
		copy(newData, wb.data)
		wb.data = newData
	}

	// Write data at offset
	n := copy(wb.data[offset:], data)
	wb.size = uint64(len(wb.data))
	wb.modified = true

	return n, nil
}

func (wb *WriteBuffer) Read(dest []byte, offset int64) (int, error) {
	wb.mu.RLock()
	defer wb.mu.RUnlock()

	if offset >= int64(len(wb.data)) {
		return 0, io.EOF
	}

	n := copy(dest, wb.data[offset:])
	return n, nil
}

func (wb *WriteBuffer) Size() uint64 {
	wb.mu.RLock()
	defer wb.mu.RUnlock()
	return wb.size
}

func (wb *WriteBuffer) IsModified() bool {
	wb.mu.RLock()
	defer wb.mu.RUnlock()
	return wb.modified
}

func (wb *WriteBuffer) GetData() []byte {
	wb.mu.RLock()
	defer wb.mu.RUnlock()
	result := make([]byte, len(wb.data))
	copy(result, wb.data)
	return result
}

// WritableFileHandle represents a file handle for write operations
type WritableFileHandle struct {
	path   string
	client *StorachaClient
	buffer *WriteBuffer
	debug  bool
}

func NewWritableFileHandle(path string, client *StorachaClient, debug bool) *WritableFileHandle {
	return &WritableFileHandle{
		path:   path,
		client: client,
		buffer: NewWriteBuffer(),
		debug:  debug,
	}
}

// Write implements fs.FileWriter
func (wfh *WritableFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := wfh.buffer.Write(data, off)
	if err != nil {
		if wfh.debug {
			fmt.Printf("Write error: %v\n", err)
		}
		return 0, syscall.EIO
	}

	if wfh.debug {
		fmt.Printf("Wrote %d bytes at offset %d\n", n, off)
	}

	return uint32(n), 0
}

// Flush implements fs.FileFlusher
func (wfh *WritableFileHandle) Flush(ctx context.Context) syscall.Errno {
	if wfh.debug {
		fmt.Printf("Flushing file: %s\n", wfh.path)
	}

	// TODO: Implement actual upload to Storacha when buffer is flushed
	// For now, just mark as successfully flushed

	return 0
}

// Release implements fs.FileReleaser
func (wfh *WritableFileHandle) Release(ctx context.Context) syscall.Errno {
	if wfh.debug {
		fmt.Printf("Releasing file: %s\n", wfh.path)
	}

	// TODO: Final upload to Storacha on file close if there are pending changes

	return 0
}

// StorachaDir helper methods
func (d *StorachaDir) GetClient() *StorachaClient { return d.client }
func (d *StorachaDir) GetTree() *Tree             { return d.tree }
func (d *StorachaDir) GetDebug() bool             { return d.debug }
func (d *StorachaDir) GetBasePath() string        { return d.dir }

// Create implements fs.NodeCreater for creating new files in directories
func (d *StorachaDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if d.debug {
		fmt.Printf("Creating file: %s\n", name)
	}

	// Create new StorachaFile
	child := &StorachaFile{
		content: []byte{},
		cid:     "",
		path:    d.dir + "/" + name,
		client:  d.client,
		debug:   d.debug,
	}

	// Create writable file handle
	wfh := NewWritableFileHandle(child.path, d.client, d.debug)

	stable := fs.StableAttr{
		Mode: fuse.S_IFREG,
	}

	childNode := d.NewInode(ctx, child, stable)

	return childNode, wfh, fuse.FOPEN_DIRECT_IO, 0
}

// Mkdir implements fs.NodeMkdirer for creating new directories
func (d *StorachaDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.debug {
		fmt.Printf("Creating directory: %s\n", name)
	}

	// Create new StorachaDir
	child := &StorachaDir{
		client: d.client,
		tree:   d.tree,
		debug:  d.debug,
		dir:    d.dir + "/" + name,
	}

	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
	}

	childNode := d.NewInode(ctx, child, stable)

	return childNode, 0
}
