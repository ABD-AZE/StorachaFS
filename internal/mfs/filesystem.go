// Package mfs provides FUSE filesystem implementation backed by kubo MFS
package mfs

import (
	"context"
	"log"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// MutableFS is a mutable FUSE filesystem backed by kubo MFS
type MutableFS struct {
	fs.Inode
	client *Client
	path   string // Path relative to MFS root
	debug  bool
}

// NewMutableFS creates a new mutable filesystem
func NewMutableFS(client *Client, debug bool) *MutableFS {
	return &MutableFS{
		client: client,
		path:   "/",
		debug:  debug,
	}
}

var _ = (fs.NodeReaddirer)((*MutableFS)(nil))
var _ = (fs.NodeLookuper)((*MutableFS)(nil))
var _ = (fs.NodeGetattrer)((*MutableFS)(nil))
var _ = (fs.NodeMkdirer)((*MutableFS)(nil))
var _ = (fs.NodeCreater)((*MutableFS)(nil))
var _ = (fs.NodeUnlinker)((*MutableFS)(nil))
var _ = (fs.NodeRmdirer)((*MutableFS)(nil))
var _ = (fs.NodeRenamer)((*MutableFS)(nil))

func (m *MutableFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.Atime = uint64(time.Now().Unix())
	out.Mtime = out.Atime
	out.Ctime = out.Atime
	return 0
}

func (m *MutableFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := m.client.List(ctx, m.path)
	if err != nil {
		if m.debug {
			log.Printf("Readdir error for %s: %v", m.path, err)
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

func (m *MutableFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := filepath.Join(m.path, name)

	stat, err := m.client.Stat(ctx, childPath)
	if err != nil {
		if m.debug {
			log.Printf("Lookup not found: %s/%s: %v", m.path, name, err)
		}
		return nil, syscall.ENOENT
	}

	if stat.Type == "directory" {
		out.Mode = fuse.S_IFDIR | 0755
		child := &MutableDir{
			client: m.client,
			path:   childPath,
			debug:  m.debug,
		}
		return m.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	out.Mode = fuse.S_IFREG | 0644
	out.Size = stat.Size
	child := &MutableFile{
		client: m.client,
		path:   childPath,
		size:   stat.Size,
		debug:  m.debug,
	}
	return m.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (m *MutableFS) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := filepath.Join(m.path, name)

	if err := m.client.Mkdir(ctx, childPath); err != nil {
		if m.debug {
			log.Printf("Mkdir error: %s: %v", childPath, err)
		}
		return nil, syscall.EIO
	}

	out.Mode = fuse.S_IFDIR | mode
	child := &MutableDir{
		client: m.client,
		path:   childPath,
		debug:  m.debug,
	}
	return m.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
}

func (m *MutableFS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath := filepath.Join(m.path, name)

	// Create empty file
	if err := m.client.Write(ctx, childPath, []byte{}, 0, true, true); err != nil {
		if m.debug {
			log.Printf("Create error: %s: %v", childPath, err)
		}
		return nil, nil, 0, syscall.EIO
	}

	out.Mode = fuse.S_IFREG | mode
	out.Size = 0

	child := &MutableFile{
		client: m.client,
		path:   childPath,
		size:   0,
		debug:  m.debug,
	}

	inode = m.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG})
	handle := &MutableFileHandle{
		client: m.client,
		path:   childPath,
		debug:  m.debug,
	}

	return inode, handle, fuse.FOPEN_DIRECT_IO, 0
}

func (m *MutableFS) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := filepath.Join(m.path, name)

	if err := m.client.Remove(ctx, childPath, false); err != nil {
		if m.debug {
			log.Printf("Unlink error: %s: %v", childPath, err)
		}
		return syscall.EIO
	}
	return 0
}

func (m *MutableFS) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := filepath.Join(m.path, name)

	if err := m.client.Remove(ctx, childPath, true); err != nil {
		if m.debug {
			log.Printf("Rmdir error: %s: %v", childPath, err)
		}
		return syscall.EIO
	}
	return 0
}

func (m *MutableFS) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := filepath.Join(m.path, name)

	// Get new parent path
	var newParentPath string
	switch p := newParent.(type) {
	case *MutableFS:
		newParentPath = p.path
	case *MutableDir:
		newParentPath = p.path
	default:
		return syscall.EINVAL
	}

	newPath := filepath.Join(newParentPath, newName)

	if err := m.client.Rename(ctx, oldPath, newPath); err != nil {
		if m.debug {
			log.Printf("Rename error: %s -> %s: %v", oldPath, newPath, err)
		}
		return syscall.EIO
	}
	return 0
}

// MutableDir represents a mutable directory in MFS
type MutableDir struct {
	fs.Inode
	client *Client
	path   string
	debug  bool
}

var _ = (fs.NodeReaddirer)((*MutableDir)(nil))
var _ = (fs.NodeLookuper)((*MutableDir)(nil))
var _ = (fs.NodeGetattrer)((*MutableDir)(nil))
var _ = (fs.NodeMkdirer)((*MutableDir)(nil))
var _ = (fs.NodeCreater)((*MutableDir)(nil))
var _ = (fs.NodeUnlinker)((*MutableDir)(nil))
var _ = (fs.NodeRmdirer)((*MutableDir)(nil))
var _ = (fs.NodeRenamer)((*MutableDir)(nil))

func (d *MutableDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.Atime = uint64(time.Now().Unix())
	out.Mtime = out.Atime
	out.Ctime = out.Atime
	return 0
}

func (d *MutableDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := d.client.List(ctx, d.path)
	if err != nil {
		if d.debug {
			log.Printf("Readdir error for %s: %v", d.path, err)
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

func (d *MutableDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := filepath.Join(d.path, name)

	stat, err := d.client.Stat(ctx, childPath)
	if err != nil {
		return nil, syscall.ENOENT
	}

	if stat.Type == "directory" {
		out.Mode = fuse.S_IFDIR | 0755
		child := &MutableDir{
			client: d.client,
			path:   childPath,
			debug:  d.debug,
		}
		return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	out.Mode = fuse.S_IFREG | 0644
	out.Size = stat.Size
	child := &MutableFile{
		client: d.client,
		path:   childPath,
		size:   stat.Size,
		debug:  d.debug,
	}
	return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (d *MutableDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := filepath.Join(d.path, name)

	if err := d.client.Mkdir(ctx, childPath); err != nil {
		if d.debug {
			log.Printf("Mkdir error: %s: %v", childPath, err)
		}
		return nil, syscall.EIO
	}

	out.Mode = fuse.S_IFDIR | mode
	child := &MutableDir{
		client: d.client,
		path:   childPath,
		debug:  d.debug,
	}
	return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
}

func (d *MutableDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath := filepath.Join(d.path, name)

	if err := d.client.Write(ctx, childPath, []byte{}, 0, true, true); err != nil {
		if d.debug {
			log.Printf("Create error: %s: %v", childPath, err)
		}
		return nil, nil, 0, syscall.EIO
	}

	out.Mode = fuse.S_IFREG | mode
	out.Size = 0

	child := &MutableFile{
		client: d.client,
		path:   childPath,
		size:   0,
		debug:  d.debug,
	}

	inode = d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG})
	handle := &MutableFileHandle{
		client: d.client,
		path:   childPath,
		debug:  d.debug,
	}

	return inode, handle, fuse.FOPEN_DIRECT_IO, 0
}

func (d *MutableDir) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := filepath.Join(d.path, name)

	if err := d.client.Remove(ctx, childPath, false); err != nil {
		if d.debug {
			log.Printf("Unlink error: %s: %v", childPath, err)
		}
		return syscall.EIO
	}
	return 0
}

func (d *MutableDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := filepath.Join(d.path, name)

	if err := d.client.Remove(ctx, childPath, true); err != nil {
		if d.debug {
			log.Printf("Rmdir error: %s: %v", childPath, err)
		}
		return syscall.EIO
	}
	return 0
}

func (d *MutableDir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := filepath.Join(d.path, name)

	var newParentPath string
	switch p := newParent.(type) {
	case *MutableFS:
		newParentPath = p.path
	case *MutableDir:
		newParentPath = p.path
	default:
		return syscall.EINVAL
	}

	newPath := filepath.Join(newParentPath, newName)

	if err := d.client.Rename(ctx, oldPath, newPath); err != nil {
		if d.debug {
			log.Printf("Rename error: %s -> %s: %v", oldPath, newPath, err)
		}
		return syscall.EIO
	}
	return 0
}

// MutableFile represents a mutable file in MFS
type MutableFile struct {
	fs.Inode
	client *Client
	path   string
	size   uint64
	debug  bool
}

var _ = (fs.NodeGetattrer)((*MutableFile)(nil))
var _ = (fs.NodeOpener)((*MutableFile)(nil))
var _ = (fs.NodeReader)((*MutableFile)(nil))
var _ = (fs.NodeWriter)((*MutableFile)(nil))
var _ = (fs.NodeSetattrer)((*MutableFile)(nil))

func (f *MutableFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Refresh size from MFS
	stat, err := f.client.Stat(ctx, f.path)
	if err == nil {
		f.size = stat.Size
	}

	out.Mode = fuse.S_IFREG | 0644
	out.Size = f.size
	out.Atime = uint64(time.Now().Unix())
	out.Mtime = out.Atime
	out.Ctime = out.Atime
	return 0
}

func (f *MutableFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Handle O_TRUNC flag - truncate file to 0
	if flags&syscall.O_TRUNC != 0 {
		if err := f.client.Truncate(ctx, f.path, 0); err != nil {
			if f.debug {
				log.Printf("Truncate on open error for %s: %v", f.path, err)
			}
			return nil, 0, syscall.EIO
		}
		f.size = 0
	}

	return &MutableFileHandle{
		client: f.client,
		path:   f.path,
		debug:  f.debug,
	}, fuse.FOPEN_DIRECT_IO, 0
}

func (f *MutableFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(f.size) {
		return fuse.ReadResultData(nil), 0
	}

	length := int64(len(dest))
	if off+length > int64(f.size) {
		length = int64(f.size) - off
	}

	data, err := f.client.Read(ctx, f.path, off, length)
	if err != nil {
		if f.debug {
			log.Printf("Read error for %s: %v", f.path, err)
		}
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data), 0
}

func (f *MutableFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if err := f.client.Write(ctx, f.path, data, off, true, false); err != nil {
		if f.debug {
			log.Printf("Write error for %s: %v", f.path, err)
		}
		return 0, syscall.EIO
	}

	// Update size if we wrote past end
	newSize := uint64(off) + uint64(len(data))
	if newSize > f.size {
		f.size = newSize
	}

	return uint32(len(data)), 0
}

func (f *MutableFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		if err := f.client.Truncate(ctx, f.path, sz); err != nil {
			if f.debug {
				log.Printf("Truncate error for %s: %v", f.path, err)
			}
			return syscall.EIO
		}
		f.size = sz
	}

	out.Mode = fuse.S_IFREG | 0644
	out.Size = f.size
	return 0
}

// MutableFileHandle handles file I/O operations
type MutableFileHandle struct {
	client *Client
	path   string
	debug  bool
}

var _ = (fs.FileReader)((*MutableFileHandle)(nil))
var _ = (fs.FileWriter)((*MutableFileHandle)(nil))
var _ = (fs.FileFlusher)((*MutableFileHandle)(nil))

func (h *MutableFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	stat, err := h.client.Stat(ctx, h.path)
	if err != nil {
		return nil, syscall.EIO
	}

	if off >= int64(stat.Size) {
		return fuse.ReadResultData(nil), 0
	}

	length := int64(len(dest))
	if off+length > int64(stat.Size) {
		length = int64(stat.Size) - off
	}

	data, err := h.client.Read(ctx, h.path, off, length)
	if err != nil {
		if h.debug {
			log.Printf("Read error for %s: %v", h.path, err)
		}
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data), 0
}

func (h *MutableFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if err := h.client.Write(ctx, h.path, data, off, true, false); err != nil {
		if h.debug {
			log.Printf("Write error for %s: %v", h.path, err)
		}
		return 0, syscall.EIO
	}

	return uint32(len(data)), 0
}

func (h *MutableFileHandle) Flush(ctx context.Context) syscall.Errno {
	// MFS writes are immediate, no explicit flush needed
	return 0
}
