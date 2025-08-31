package fuse

import (
    "bytes"
    "context"
    "hash/fnv"
    "io"
    "log"
    "net/http"
    "strings"
    "sync"
    "syscall"
    "time"

    "github.com/PuerkitoBio/goquery"
    "github.com/hanwen/go-fuse/v2/fs"
    "github.com/hanwen/go-fuse/v2/fuse"
)

// --- Directory FS ---

type StorachaFS struct {
    fs.Inode
    cid    string
    debug  bool
    cache  []fuse.DirEntry
    cached bool
    mu     sync.Mutex
}

func NewStorachaFS(rootCID string, debug bool) *StorachaFS {
    return &StorachaFS{
        cid:   rootCID,
        debug: debug,
    }
}

// --- Interfaces ---
var _ = (fs.NodeLookuper)((*StorachaFS)(nil))
var _ = (fs.NodeReaddirer)((*StorachaFS)(nil))
var _ = (fs.NodeGetattrer)((*StorachaFS)(nil))
var _ = (fs.NodeStatfser)((*StorachaFS)(nil))

// Getattr for directory
func (sfs *StorachaFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
    out.Mode = fuse.S_IFDIR | 0555
    return 0
}

// Statfs for directory (rsync needs this)
func (sfs *StorachaFS) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
    out.Blocks = 1e9
    out.Bfree = 1e9
    out.Bavail = 1e9
    out.Bsize = 4096
    out.Frsize = 4096
    out.NameLen = 255
    return 0
}

// Readdir lists directory contents
func (sfs *StorachaFS) Readdir(ctx context.Context, fh fs.FileHandle) (fs.DirStream, syscall.Errno) {
    sfs.mu.Lock()
    defer sfs.mu.Unlock()

    if sfs.cached {
        return fs.NewListDirStream(sfs.cache), 0
    }

    if sfs.debug {
        log.Printf("Readdir called for CID %s", sfs.cid)
    }

    url := "https://storacha.link/ipfs/" + sfs.cid + "/"
    resp, err := http.Get(url)
    if err != nil {
        return nil, syscall.ENOENT
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, syscall.ENOENT
    }

    doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
    if err != nil {
        return nil, syscall.ENOENT
    }

    var entries []fuse.DirEntry
    doc.Find("a").Each(func(i int, s *goquery.Selection) {
        href, _ := s.Attr("href")
        href = strings.Split(href, "?")[0]
        if href == "" || href == "../" {
            return
        }

        parts := strings.Split(strings.Trim(href, "/"), "/")
        if len(parts) < 2 || parts[0] != "ipfs" {
            return
        }

        name := parts[len(parts)-1]
        if name == "" {
            return
        }

        isDir := strings.HasSuffix(href, "/")
        mode := uint32(fuse.S_IFREG | 0444)
        if isDir {
            mode = fuse.S_IFDIR | 0555
        }

        entries = append(entries, fuse.DirEntry{
            Ino:  hashInode(sfs.cid + "/" + name),
            Name: name,
            Mode: mode,
        })
    })

    sfs.cache = entries
    sfs.cached = true
    return fs.NewListDirStream(entries), 0
}

// --- File FS ---

type StorachaFile struct {
    fs.Inode
    name  string
    cid   string
    data  []byte
    debug bool
}

var _ = (fs.NodeGetattrer)((*StorachaFile)(nil))
var _ = (fs.NodeReader)((*StorachaFile)(nil))
var _ = (fs.NodeOpener)((*StorachaFile)(nil))

func NewStorachaFile(name, cid string, debug bool) *StorachaFile {
    return &StorachaFile{name: name, cid: cid, debug: debug}
}

func (sf *StorachaFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
    if sf.debug {
        log.Printf("Getattr called for file %s", sf.name)
    }
    out.Mode = fuse.S_IFREG | 0444
    out.Size = uint64(len(sf.data))
    out.Mtime = uint64(time.Now().Unix())
    out.Atime = out.Mtime
    out.Ctime = out.Mtime
    return 0
}

// Lazy fetch file content on Open
func (sf *StorachaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
    if sf.data == nil {
        if sf.debug {
            log.Printf("Fetching file %s from CID %s", sf.name, sf.cid)
        }
        url := "https://storacha.link/ipfs/" + sf.cid
        resp, err := http.Get(url)
        if err != nil {
            return nil, 0, syscall.ENOENT
        }
        defer resp.Body.Close()
        data, _ := io.ReadAll(resp.Body)
        sf.data = data
    }
    return sf, fuse.FOPEN_KEEP_CACHE, 0
}

func (sf *StorachaFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
    if off >= int64(len(sf.data)) {
        return fuse.ReadResultData(nil), 0
    }
    end := off + int64(len(dest))
    if end > int64(len(sf.data)) {
        end = int64(len(sf.data))
    }
    return fuse.ReadResultData(sf.data[off:end]), 0
}

// --- Helpers ---

func hashInode(s string) uint64 {
    h := fnv.New64a()
    h.Write([]byte(s))
    return h.Sum64()
}
