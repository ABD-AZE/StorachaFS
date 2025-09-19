// cmd/storachafs/mount.go
package storachafs

import (
	"fmt"
	"log"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	"github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
)

var (
	entryTTL time.Duration
	attrTTL  time.Duration
	debug    bool
)

var mountCmd = &cobra.Command{
	Use:   "mount [CID] [mountpoint]",
	Short: "Mount a Storacha space at a local directory",
	Long:  `Mount a Storacha space, identified by its CID, at a local directory.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		cid := args[0]
		mnt := args[1]

		root := fuse.NewStorachaFS(cid, debug)
		opts := &fs.Options{
			MountOptions: fusefs.MountOptions{
				FsName: fmt.Sprintf("storachafs-%s", cid),
				Name:   "storachafs",
			},
			EntryTimeout: &entryTTL,
			AttrTimeout:  &attrTTL,
		}

		server, err := fs.Mount(mnt, root, opts)
		if err != nil {
			log.Fatalf("mount: %v", err)
		}
		log.Printf("mounted %s at %s", cid, mnt)
		server.Wait()
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.Flags().DurationVar(&entryTTL, "entry-ttl", time.Second, "kernel dentry TTL")
	mountCmd.Flags().DurationVar(&attrTTL, "attr-ttl", time.Second, "kernel attr TTL")
	mountCmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")
}
