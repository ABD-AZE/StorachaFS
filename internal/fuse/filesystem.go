package fuse

import (
	"context"
	"fmt"
	"sync"
	"syscall"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// StorachaFS represents the root of the FUSE filesystem
type StorachaFS struct {
	fs.Inode
	rootCID cid.Cid
	debug   bool
	store  *BlockStore
	tree    *Tree
}

// IPFS access layer
/* Fetch blocks via GetBlock
   Provide IPLD decoding via LinkSystem
*/
type BlockStore struct {
	blockSvc blockservice.BlockService
	lsys     ipld.LinkSystem
}

// Tree represents the IPFS/IPLD tree structure
type Tree struct {
	root cid.Cid
	cache map[string]*TreeNode
	mu sync.RWMutex
}

type NodeKind int

const (
	NodeKindFile NodeKind = iota
	NodeKindDir
	NodeKindSymlink
)

type TreeNode struct {
	Name string
	CID  cid.Cid
	Kind NodeKind
	Size uint64
}

// StorachaFile represents a file in the filesystem
type StorachaFile struct {
	fs.Inode
	cid         cid.Cid
	path        string
	store       *BlockStore
	debug       bool
}

// StorachaDir represents a directory in the filesystem
type StorachaDir struct {
	fs.Inode
	cid   cid.Cid
	tree  *Tree
	store *BlockStore
	debug bool
}

// NewStorachaFS creates a new StorachaFS instance
func NewStorachaFS(rootCID string, debug bool) (*StorachaFS, error) {
	c, err := cid.Parse(rootCID)

	if err != nil {
		return nil, fmt.Errorf("invalid CID: %v", err)
	}
	return &StorachaFS{
		rootCID: c,
		debug:   debug,
		store:   &BlockStore{},
		tree:    &Tree{},
	}, nil
}

// Ensure StorachaFS implements the necessary interfaces
var _ = (fs.NodeReaddirer)((*StorachaFS)(nil))
var _ = (fs.NodeLookuper)((*StorachaFS)(nil))

// Readdir implements fs.NodeReaddirer
func (root *StorachaFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// TODO
	return nil, 0
}

// Lookup implements fs.NodeLookuper
func (root *StorachaFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO
	return nil, syscall.ENOENT
}

// Getattr implements fs.NodeGetattrer
func (root *StorachaFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// TODO
	return 0
}

// Open implements fs.NodeOpener for files
func (f *StorachaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// TODO
	return nil, fuse.FOPEN_DIRECT_IO, 0
}

// Read implements fs.NodeReader for files
func (f *StorachaFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// TODO
	return nil, 0
}

// Getattr implements fs.NodeGetattrer for files
func (f *StorachaFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// TODO
	return 0
}
