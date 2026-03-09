// cmd/storachafs/mount.go
package storachafs

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"

	storachafuse "github.com/ABD-AZE/StorachaFS/internal/fuse"
	"github.com/ABD-AZE/StorachaFS/internal/mfs"
	"github.com/ABD-AZE/StorachaFS/internal/storacha"
)

var (
	entryTTL time.Duration
	attrTTL  time.Duration
	debug    bool

	// Mutable mode flags
	mutable  bool
	spaceDID string
)

var mountCmd = &cobra.Command{
	Use:   "mount [cid] <mountpoint>",
	Short: "Mount IPFS content at the specified mountpoint",
	Long: `Mount IPFS content from Storacha/IPFS at a local directory.

This command uses a local kubo (IPFS) daemon for content retrieval and UnixFS DAG traversals.
Make sure 'ipfs daemon' is running before using this command.

Read-only mode (default):
  storachafs mount bafybeidd2gyhagleh47qeg77xqndy2qy3yzn4vkxmk775bg2t5lpuy7pcu /mnt/ipfs

Mutable mode (--mutable):
  storachafs mount --mutable bafybeidd2gyhagleh47qeg77xqndy2qy3yzn4vkxmk775bg2t5lpuy7pcu /mnt/ipfs
  
  In mutable mode, changes are stored locally using kubo's MFS. When you unmount,
  all changes are automatically uploaded to Storacha.

Mutable mode without base CID (fresh start):
  storachafs mount --mutable /mnt/ipfs
  
  This creates an empty mutable filesystem. Perfect for creating new content from scratch.

Storacha options (for mutable mode):
  --space-did    Storacha space DID for uploads (uses default space if not specified)

Example with Storacha space:
  storachafs mount --mutable --space-did did:key:z6Mk... bafybei... /mnt/ipfs`,
	Args: cobra.RangeArgs(1, 2),
	Run:  runMount,
}

func runMount(cmd *cobra.Command, args []string) {
	var cid, mountpoint string

	if len(args) == 1 {
		// Only mountpoint provided (mutable mode without base CID)
		cid = ""
		mountpoint = args[0]
	} else {
		// Both CID and mountpoint provided
		cid = args[0]
		mountpoint = args[1]
	}

	// Validate: read-only mode requires a CID
	if !mutable && cid == "" {
		log.Fatalf("Read-only mode requires a CID. Use --mutable for an empty filesystem.")
	}

	// Create mount point if it doesn't exist
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		log.Fatalf("Failed to create mount point %s: %v", mountpoint, err)
	}

	if mutable {
		runMutableMount(cid, mountpoint)
	} else {
		runReadOnlyMount(cid, mountpoint)
	}
}

func runReadOnlyMount(cid, mountpoint string) {
	log.Printf("Mounting CID %s at %s (read-only)", cid, mountpoint)
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

	log.Printf("Filesystem mounted successfully (read-only). Press Ctrl+C to unmount.")

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

	// Wait for the server to finish
	server.Wait()
	log.Println("Filesystem unmounted.")
}

func runMutableMount(cid, mountpoint string) {
	if cid != "" {
		log.Printf("Mounting CID %s at %s (mutable mode)", cid, mountpoint)
	} else {
		log.Printf("Mounting empty filesystem at %s (mutable mode)", mountpoint)
	}
	if debug {
		log.Println("Debug mode enabled")
	}

	ctx := context.Background()

	// Initialize MFS client
	mfsClient := mfs.NewClient(debug, "/storachafs-"+time.Now().Format("20060102-150405"))

	// Initialize MFS with the base CID (empty string = empty directory)
	if err := mfsClient.Initialize(ctx, cid); err != nil {
		log.Fatalf("Failed to initialize MFS: %v", err)
	}
	if cid != "" {
		log.Printf("MFS initialized with base CID: %s", cid)
	} else {
		log.Println("MFS initialized with empty directory")
	}

	// Create the mutable FUSE filesystem
	root := mfs.NewMutableFS(mfsClient, debug)

	// Configure FUSE options
	opts := &fs.Options{
		AttrTimeout:  &attrTTL,
		EntryTimeout: &entryTTL,
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      debug,
			FsName:     "storachafs-mutable",
			Name:       "storachafs",
		},
	}

	// Mount the filesystem
	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		mfsClient.Cleanup(ctx)
		log.Fatalf("Failed to mount: %v", err)
	}

	log.Printf("Mutable filesystem mounted successfully. Press Ctrl+C to unmount and upload changes.")
	if spaceDID != "" {
		log.Printf("Changes will be uploaded to Storacha space: %s", spaceDID)
	} else {
		log.Println("Using default Storacha space for uploads.")
	}

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

	// Wait for the server to finish
	server.Wait()

	// After unmount, commit changes to Storacha
	commitChangesToStoracha(ctx, mfsClient)
}

func commitChangesToStoracha(ctx context.Context, mfsClient *mfs.Client) {
	if !mfsClient.IsModified() {
		log.Println("No changes made. Skipping upload.")
		mfsClient.Cleanup(ctx)
		return
	}

	log.Println("Committing changes to Storacha...")

	// Get the new root CID from MFS
	newCID, err := mfsClient.GetRootCID(ctx)
	if err != nil {
		log.Printf("Error getting new CID: %v", err)
		mfsClient.Cleanup(ctx)
		return
	}
	log.Printf("New root CID: %s", newCID)

	// Initialize Storacha bridge
	bridge, err := storacha.NewBridge(storacha.BridgeConfig{
		SpaceDID: spaceDID,
		Debug:    debug,
	})
	if err != nil {
		log.Printf("Warning: Could not initialize Storacha bridge: %v", err)
		log.Printf("Your changes are saved locally with CID: %s", newCID)
		log.Printf("You can access via local IPFS: ipfs get %s", newCID)
		mfsClient.Cleanup(ctx)
		return
	}
	defer bridge.Close()

	// Upload CID to Storacha (bridge will fetch from local IPFS and upload)
	log.Println("Uploading to Storacha (this may take a moment)...")
	uploadedCID, err := bridge.UploadCID(ctx, newCID)
	if err != nil {
		log.Printf("Error uploading to Storacha: %v", err)
		log.Printf("Your changes are saved locally with CID: %s", newCID)
		log.Printf("You can access via local IPFS: ipfs get %s", newCID)
		mfsClient.Cleanup(ctx)
		return
	}

	log.Printf("Successfully uploaded to Storacha!")
	log.Printf("New CID: %s", uploadedCID)
	log.Printf("Access via: https://w3s.link/ipfs/%s", uploadedCID)

	// Cleanup
	mfsClient.Cleanup(ctx)
	log.Println("Cleanup complete.")
}

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.Flags().DurationVar(&entryTTL, "entry-ttl", time.Second, "kernel dentry TTL")
	mountCmd.Flags().DurationVar(&attrTTL, "attr-ttl", time.Second, "kernel attr TTL")
	mountCmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")

	// Mutable mode flags
	mountCmd.Flags().BoolVar(&mutable, "mutable", false, "enable mutable filesystem mode (changes uploaded to Storacha on unmount)")
	mountCmd.Flags().StringVar(&spaceDID, "space-did", "", "Storacha space DID for uploads (optional)")
}
