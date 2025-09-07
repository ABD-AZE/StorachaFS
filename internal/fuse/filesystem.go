package fuse

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// ---------- Storacha client abstraction ----------

type FileEntry struct {
	Name string
	Dir  bool
	Size uint64
	CID  string
}

type Tree map[string][]FileEntry // key = dir path ("" for root)

// contract for interacting with IPFS/Storacha content
type StorachaClient interface {
	ListTree(cid string) (Tree, error)
	OpenReader(cid, p string) (io.ReadSeeker, uint64, error)
}

// Real Storacha client implementation
type storachaClient struct {
	debug bool
}

func NewStorachaClient(debug bool) StorachaClient {
	return &storachaClient{debug: debug}
}

func (c *storachaClient) ListTree(cid string) (Tree, error) {
	t := make(Tree)
	err := c.listTreeRecursive(cid, "", t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// currently uses html based parsing of the page obtained by querying
func (c *storachaClient) listTreeRecursive(cid, dirPath string, tree Tree) error {
	if c.debug {
		log.Printf("Listing directory CID %s at path %s", cid, dirPath)
	}

	url := "https://storacha.link/ipfs/" + cid + "/"
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return err
	}

	var entries []FileEntry
	
	// First, collect all the direct CID links (these contain the actual file CIDs)
	cidMap := make(map[string]string) // filename -> CID
	doc.Find("a.ipfs-hash").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		href = strings.Split(href, "?")[0] // strip query params
		
		parts := strings.Split(strings.Trim(href, "/"), "/")
		if len(parts) >= 2 && parts[0] == "ipfs" {
			cid := parts[1]
			// Extract filename from the query parameter or link text
			filename := ""
			if fullHref, exists := s.Attr("href"); exists && strings.Contains(fullHref, "filename=") {
				parts := strings.Split(fullHref, "filename=")
				if len(parts) > 1 {
					filename = parts[1]
				}
			}
			if filename == "" {
				filename = strings.TrimSpace(s.Text())
			}
			if filename != "" {
				cidMap[filename] = cid
			}
		}
	})
	
	// Then, process the file path links to get the filenames
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		// Skip if this is a CID hash link (we already processed these)
		if s.HasClass("ipfs-hash") {
			return
		}
		
		href, _ := s.Attr("href")
		href = strings.Split(href, "?")[0] // strip query params
		if href == "" || href == "../" {
			return
		}

		parts := strings.Split(strings.Trim(href, "/"), "/")
		if len(parts) < 2 || parts[0] != "ipfs" {
			return
		}

		// Only process links that have a path component (file path links)
		// Skip direct CID links that don't have a path
		if len(parts) < 3 {
			return
		}

		// Get the actual filename from the link text
		name := strings.TrimSpace(s.Text())
		if name == "" || name == "../" {
			return
		}
		
		// Get the corresponding CID from our map
		childCID, exists := cidMap[name]
		if !exists {
			// Fallback: use the directory CID (this shouldn't happen in normal cases)
			childCID = parts[1]
		}

		isDir := strings.HasSuffix(href, "/")
		
		// Get file size for non-directory entries
		var size uint64
		if !isDir {
			url := "https://storacha.link/ipfs/" + childCID
			resp, err := http.Head(url)
			if err == nil {
				size = uint64(resp.ContentLength)
				if err := resp.Body.Close(); err != nil {
					log.Printf("Failed to close response body: %v", err)
				}
			}
		}

		entries = append(entries, FileEntry{
			Name: name,
			Dir:  isDir,
			Size: size,
			CID:  childCID,
		})
	})

	tree[dirPath] = entries
	return nil
}

// The actual reader logic goes here, This function is called by Open method of StorachaFile
func (c *storachaClient) OpenReader(cid, p string) (io.ReadSeeker, uint64, error) {
	if c.debug {
		log.Printf("Opening file CID %s at path %s", cid, p)
	}

	// For individual files, use the CID directly - each file has its own CID in IPFS
	url := "https://storacha.link/ipfs/" + cid
	if c.debug {
		log.Printf("Fetching URL: %s", url)
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return newBytesReadSeeker(data), uint64(len(data)), nil
}

// Simple ReadSeeker over a byte slice
// Represents in-memory storage of files (only works for small files)
type bytesRS struct {
	b   []byte
	off int64
}

func newBytesReadSeeker(b []byte) *bytesRS {
	return &bytesRS{b: b}
}

// This is the method called by OS when cat or other command reads the file
func (r *bytesRS) Read(p []byte) (int, error) {
	if r.off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += int64(n)
	return n, nil
}
 
// This is the method called by OS when cat or other command seeks the file
func (r *bytesRS) Seek(off int64, whence int) (int64, error) {
	var n int64
	switch whence {
	case io.SeekStart:
		n = off
	case io.SeekCurrent:
		n = r.off + off
	case io.SeekEnd:
		n = int64(len(r.b)) + off
	default:
		return 0, errors.New("bad whence")
	}
	if n < 0 {
		return 0, errors.New("negative position")
	}
	r.off = n
	return n, nil
}

// ---------- go-fuse nodes ----------

// StorachaFS is the root node of the filesystem
type StorachaFS struct {
	fs.Inode
	cid    string
	client StorachaClient
	tree   Tree
	debug  bool
}

func NewStorachaFS(rootCID string, debug bool) *StorachaFS {
	client := NewStorachaClient(debug)
	tree, err := client.ListTree(rootCID)
	if err != nil {
		log.Printf("Failed to list tree for CID %s: %v", rootCID, err)
		tree = make(Tree) // Empty tree on error
	}
	return &StorachaFS{
		cid:    rootCID,
		client: client,
		tree:   tree,
		debug:  debug,
	}
}

var _ = (fs.NodeLookuper)((*StorachaFS)(nil))
var _ = (fs.NodeReaddirer)((*StorachaFS)(nil))
var _ = (fs.NodeGetattrer)((*StorachaFS)(nil))
var _ = (fs.NodeStatfser)((*StorachaFS)(nil))

func (r *StorachaFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0555
	return 0
}


// currently contains fake values
func (r *StorachaFS) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Blocks = 1e9
	out.Bfree = 1e9
	out.Bavail = 1e9
	out.Bsize = 4096
	out.Frsize = 4096
	out.NameLen = 255
	return 0
}

func (r *StorachaFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupCommon(ctx, &r.Inode, r.cid, r.client, r.tree, "", name, out, r.debug)
}

func (r *StorachaFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	list := r.tree[""]
	return dirStreamFrom(list), 0
}

// StorachaDir is a directory sub-node
type StorachaDir struct {
	fs.Inode
	cid    string
	client StorachaClient
	tree   Tree
	dir    string // path from root, "" for root
	debug  bool
}

var _ = (fs.NodeLookuper)((*StorachaDir)(nil))
var _ = (fs.NodeReaddirer)((*StorachaDir)(nil))
var _ = (fs.NodeGetattrer)((*StorachaDir)(nil))

func (d *StorachaDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0555
	return 0
}

func (d *StorachaDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupCommon(ctx, &d.Inode, d.cid, d.client, d.tree, d.dir, name, out, d.debug)
}

func (d *StorachaDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	list := d.tree[d.dir]
	return dirStreamFrom(list), 0
}

// StorachaFile is a file sub-node
type StorachaFile struct {
	fs.Inode
	cid    string
	client StorachaClient
	path   string
	size   uint64
	debug  bool
}

var _ = (fs.NodeGetattrer)((*StorachaFile)(nil))
var _ = (fs.NodeOpener)((*StorachaFile)(nil))

func (f *StorachaFile) Getattr(ctx context.Context, h fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = f.size
	out.Mtime = uint64(time.Now().Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	return 0
}

func (f *StorachaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	rs, size, err := f.client.OpenReader(f.cid, f.path)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	return &fileHandle{rs: rs, size: size}, fuse.FOPEN_KEEP_CACHE, 0
}

// Represents an open file handle with downloaded content.
type fileHandle struct {
	fs.FileHandle
	rs   io.ReadSeeker
	size uint64
}

var _ = (fs.FileReader)((*fileHandle)(nil))

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if _, err := h.rs.Seek(off, io.SeekStart); err != nil {
		return nil, fs.ToErrno(err)
	}
	n, err := io.ReadFull(h.rs, dest)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		// Partial read is fine
		return fuse.ReadResultData(dest[:n]), 0
	}
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// // -------------------- write methods --------------------

// // For file creation and modification
// var _ = (fs.NodeCreater)((*StorachaDir)(nil))
// var _ = (fs.NodeMkdirer)((*StorachaDir)(nil))
// var _ = (fs.NodeUnlinker)((*StorachaDir)(nil))
// var _ = (fs.NodeRmdirer)((*StorachaDir)(nil))

// func (d *StorachaDir) Create(ctx context.Context, name string, mode uint32, umask uint32, flags uint32) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (d *StorachaDir) Mkdir(ctx context.Context, name string, mode uint32, umask uint32) (*fs.Inode, uint32, syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (d *StorachaDir) Unlink(ctx context.Context, name string) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (d *StorachaDir) Rmdir(ctx context.Context, name string) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (d *StorachaDir) Rename(ctx context.Context, name string, newParent *fs.Inode, newName string, flags uint32) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// var _ = (fs.FileWriter)((*StorachaFile)(nil))
// var _ = (fs.FileFlusher)((*StorachaFile)(nil))
// var _ = (fs.FileReleaser)((*StorachaFile)(nil))
// var _ = (fs.FileFsyncer)((*StorachaFile)(nil))

// func (f *StorachaFile) Write(ctx context.Context, data []byte, off int64) (int, syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (f *StorachaFile) Flush(ctx context.Context) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (f *StorachaFile) Release(ctx context.Context) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (f *StorachaFile) Fsync(ctx context.Context, flags int) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// func (f *StorachaFile) Setattr(ctx context.Context, attr *fuse.SetAttrIn, out *fuse.AttrOut) (syscall.Errno) {
//     client , _ = auth.EmailAuth(email)
    
// }

// ---------- helpers ----------

func lookupCommon(ctx context.Context, parent *fs.Inode, cid string, client StorachaClient, tree Tree, dir, name string, out *fuse.EntryOut, debug bool) (*fs.Inode, syscall.Errno) {
	full := path.Join(dir, name)
	entries := tree[dir]
	for _, e := range entries {
		if e.Name != name {
			continue
		}
		if e.Dir {
			out.Mode = fuse.S_IFDIR | 0555
			ch := parent.NewInode(ctx, &StorachaDir{cid: e.CID, client: client, tree: tree, dir: full, debug: debug}, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: hashInode(e.CID + "/" + full)})
			return ch, 0
		}
		out.Mode = fuse.S_IFREG | 0444
		out.Size = e.Size
		ch := parent.NewInode(ctx, &StorachaFile{cid: e.CID, client: client, path: "/" + full, size: e.Size, debug: debug}, fs.StableAttr{Mode: syscall.S_IFREG, Ino: hashInode(e.CID + "/" + full)})
		return ch, 0
	}
	return nil, syscall.ENOENT
}

func dirStreamFrom(list []FileEntry) fs.DirStream {
	var dirents []fuse.DirEntry
	for _, e := range list {
		mode := uint32(fuse.S_IFREG)
		if e.Dir {
			mode = fuse.S_IFDIR
		}
		dirents = append(dirents, fuse.DirEntry{
			Mode: mode,
			Name: e.Name,
			Ino:  hashInode(e.CID + "/" + e.Name),
		})
	}
	return fs.NewListDirStream(dirents)
}

func hashInode(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
