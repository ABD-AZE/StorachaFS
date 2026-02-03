// cmd/storachafs/mount.go
package storachafs

import (
"log"
"os"
"time"
"github.com/spf13/cobra"
)

var (
entryTTL time.Duration
attrTTL  time.Duration
debug    bool
)

var mountCmd = &cobra.Command{
Use:   "mount <cid> <mountpoint>",
Short: "Mount IPFS content at the specified mountpoint",
Long: "Mount IPFS content from Storacha/IPFS at a local directory.\n\nThis command uses kubo for content retrieval and UnixFS DAG traversals.\n\nExample:\n  storachafs mount bafybeidd2gyhagleh47qeg77xqndy2qy3yzn4vkxmk775bg2t5lpuy7pcu /mnt/ipfs",
Args: cobra.ExactArgs(2),
Run: func(cmd *cobra.Command, args []string) {
cid := args[0]
mountpoint := args[1]

if err := os.MkdirAll(mountpoint, 0755); err != nil {
log.Fatalf("Failed to create mount point %s: %v", mountpoint, err)
}

log.Printf("Mounting CID %s at %s", cid, mountpoint)
// Create the FUSE filesystem
},
}

func init() {
rootCmd.AddCommand(mountCmd)
mountCmd.Flags().DurationVar(&entryTTL, "entry-ttl", time.Second, "kernel dentry TTL")
mountCmd.Flags().DurationVar(&attrTTL, "attr-ttl", time.Second, "kernel attr TTL")
mountCmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")
}
