package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	"github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
)
// run eg: `./main QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG /tmp/storacha` after buidling with `go build -o main cmd/main.go`
func main() {
	log.SetFlags(0)
	if len(os.Args) < 3 {
		usage()
	}
	mountCmd(os.Args[1:])
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  storachafs <CID> <mountpoint>
`)
	os.Exit(2)
}

func mountCmd(args []string) {
	fsFlags := flag.NewFlagSet("mount", flag.ExitOnError)
	entryTTL := fsFlags.Duration("entry-ttl", time.Second, "kernel dentry TTL")
	attrTTL := fsFlags.Duration("attr-ttl", time.Second, "kernel attr TTL")
	debug := fsFlags.Bool("debug", false, "enable debug logging")
	_ = fsFlags.Parse(args)
	if fsFlags.NArg() != 2 {
		usage()
	}
	cid := fsFlags.Arg(0)
	mnt := fsFlags.Arg(1)

	root := fuse.NewStorachaFS(cid, *debug)
	opts := &fs.Options{
		MountOptions: fusefs.MountOptions{
			FsName: fmt.Sprintf("storachafs-%s", cid),
			Name:   "storachafs",
		},
		EntryTimeout: entryTTL,
		AttrTimeout:  attrTTL,
	}

	server, err := fs.Mount(mnt, root, opts)
	if err != nil {
		log.Fatalf("mount: %v", err)
	}
	log.Printf("mounted %s at %s", cid, mnt)
	server.Wait()
}
