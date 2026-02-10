// cmd/storachafs/mount.go
package storachafs

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"

	storachafuse "github.com/ABD-AZE/StorachaFS/internal/fuse"
)

var (
	entryTTL time.Duration
	attrTTL  time.Duration
	debug    bool
)

var mountCmd = &cobra.Command{
	Use:   "mount <cid> <mountpoint>",
	Short: "Mount IPFS content at the specified mountpoint",
	Long: `Mount IPFS content from Storacha/IPFS at a local directory.

This command uses a local kubo (IPFS) daemon for content retrieval and UnixFS DAG traversals.
Make sure 'ipfs daemon' is running before using this command.

Example:
  storachafs mount bafybeidd2gyhagleh47qeg77xqndy2qy3yzn4vkxmk775bg2t5lpuy7pcu /mnt/ipfs`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		cid := args[0]
		mountpoint := args[1]

		// Create mount point if it doesn't exist
		if err := os.MkdirAll(mountpoint, 0755); err != nil {
			log.Fatalf("Failed to create mount point %s: %v", mountpoint, err)
		}

		log.Printf("Mounting CID %s at %s", cid, mountpoint)
		if debug {
			log.Println("Debug mode enabled")
		}

		// Create the FUSE filesystem
		root := storachafuse.NewStorachaFS(cid, debug)

		// Configure FUSE options
		opts := &fs.Options{
			AttrTimeout:  &attrTTL,
			EntryTimeout: &entryTTL,
			MountOptions: fuse.MountOptions{
				AllowOther: false,
				Debug:      debug,
				FsName:     "storachafs",
				Name:       "storachafs",
			},
		}

		// Mount the filesystem
		server, err := fs.Mount(mountpoint, root, opts)
		if err != nil {
			log.Fatalf("Failed to mount: %v", err)
		}

		log.Printf("Filesystem mounted successfully. Press Ctrl+C to unmount.")

		// Handle graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			<-sigChan
			log.Println("\nUnmounting...")
			if err := server.Unmount(); err != nil {
				log.Printf("Warning: unmount error: %v", err)
			}
		}()

		// Wait for the server to finish (blocks until unmounted)
		server.Wait()
		log.Println("Filesystem unmounted.")
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.Flags().DurationVar(&entryTTL, "entry-ttl", time.Second, "kernel dentry TTL")
	mountCmd.Flags().DurationVar(&attrTTL, "attr-ttl", time.Second, "kernel attr TTL")
	mountCmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")
}
