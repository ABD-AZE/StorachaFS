package fuse

import (
    "bytes"
    "context"
    "io"
    "log"
    "net/http"
    "path"
    "strings"
    "syscall"

    "github.com/hanwen/go-fuse/v2/fs"
    "github.com/hanwen/go-fuse/v2/fuse"
    "github.com/PuerkitoBio/goquery"
)

type StorachaFS struct {
    fs.Inode
    cid   string
    debug bool
}

func NewStorachaFS(rootCID string, debug bool) *StorachaFS {
    return &StorachaFS{
        cid:   rootCID,
        debug: debug,
    }
}

var _ = (fs.NodeLookuper)((*StorachaFS)(nil))

func (sfs *StorachaFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
    if sfs.debug {
        log.Printf("Lookup: %s under CID %s", name, sfs.cid)
    }

    dirURL := "https://storacha.link/ipfs/" + sfs.cid + "/"
    resp, err := http.Get(dirURL)
    if err != nil {
        return nil, syscall.ENOENT
    }
    defer resp.Body.Close()

    data, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, syscall.ENOENT
    }

    doc, err := goquery.NewDocumentFromReader(bytes.NewReader(data))
    if err != nil {
        return nil, syscall.ENOENT
    }

    var childCID string
    var isDir bool

    doc.Find("a").EachWithBreak(func(i int, s *goquery.Selection) bool {
        href, _ := s.Attr("href")
        href = strings.Split(href, "?")[0] // strip query params

        // Match exact filename
        if strings.HasSuffix(href, "/"+name) || href == name {
            parts := strings.Split(strings.Trim(href, "/"), "/")
            if len(parts) >= 2 && parts[0] == "ipfs" {
                childCID = parts[1]
                isDir = strings.HasSuffix(href, "/")
                return false // found
            }
        }
        return true
    })

    if childCID == "" {
        return nil, syscall.ENOENT
    }

    // Attributes
    var node fs.InodeEmbedder
    if isDir {
        out.Attr.Mode = fuse.S_IFDIR | 0555
        node = NewStorachaFS(childCID, sfs.debug)
    } else {
        out.Attr.Mode = fuse.S_IFREG | 0444
        node = NewStorachaFile(childCID, name, sfs.debug)
    }

    // Stable ID = hash of CID+name
    ino := hashInode(childCID + "/" + name)

    child := sfs.NewPersistentInode(ctx, node, fs.StableAttr{
        Mode: out.Attr.Mode,
        Ino:  ino,
    })

    return child, 0
}

// helper for inode IDs
func hashInode(s string) uint64 {
    h := fnv.New64a()
    h.Write([]byte(s))
    return h.Sum64()
}

