// cmd/storachafs/mount.go
package storachafs

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/auth"
	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"

	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/didmailto"

	// IPLD and CAR imports
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

var (
	entryTTL       time.Duration
	attrTTL        time.Duration
	debug          bool
	email          string
	cidFlag        string
	sourcePath     string
	privateKeyPath string
	proofPath      string
	spaceDID       string
	readOnly       bool
)

var mountCmd = &cobra.Command{
	Use:   "mount [mountpoint]",
	Short: "Mount a Storacha space or upload and mount a local directory",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		mnt := args[0]

		// Validate that exactly one of --cid or --source is provided
		if (cidFlag == "" && sourcePath == "") || (cidFlag != "" && sourcePath != "") {
			log.Fatalf("You must specify exactly one of --cid (to mount existing content) or --source (to upload and mount local directory)")
		}

		// Create mount point if it doesn't exist
		if err := os.MkdirAll(mnt, 0755); err != nil {
			log.Fatalf("Failed to create mount point %s: %v", mnt, err)
		}

		var finalCID string

		// Determine authentication method and validate
		if !readOnly {
			authMethod, err := auth.GetAuthMethodFromArgs(email, privateKeyPath, proofPath, spaceDID)
			if err != nil {
				log.Fatalf("Authentication error: %v", err)
			}

			switch authMethod {
			case "email":
				log.Println("Using email authentication (interactive). NOTE: uploads with --source require pre-authorized key+proof; use --private-key + --proof for non-interactive uploads.")
			case "private_key":
				log.Println("Using private key authentication...")
				// Validate private key authentication
				var authConfig *auth.AuthConfig
				if privateKeyPath != "" && proofPath != "" && spaceDID != "" {
					authConfig = auth.LoadAuthConfigFromFlags(privateKeyPath, proofPath, spaceDID)
				} else {
					authConfig, err = auth.LoadAuthConfigFromEnv()
					if err != nil {
						log.Fatalf("Private key authentication failed: %v", err)
					}
				}
				if err := auth.ValidateAuthConfig(authConfig); err != nil {
					log.Fatalf("Authentication validation failed: %v", err)
				}
			case "none":
				log.Println("No authentication provided - mounting in read-only mode")
				log.Println("For write operations, provide authentication via:")
				log.Println("  --email for email auth, or")
				log.Println("  --private-key, --proof, --space for private key auth")
			}
		} else {
			log.Println("Mounting in read-only mode (no authentication)")
		}

		// Handle upload vs mount existing content
		if sourcePath != "" {
			// Upload directory first
			if readOnly {
				log.Fatalf("Cannot upload directory in read-only mode. Please provide authentication.")
			}

			// Validate that space DID is provided for uploads
			if spaceDID == "" {
				log.Fatalf("Space DID (--space) is required when uploading content. Please provide a valid space DID.")
			}

			log.Printf("Packing and uploading directory: %s", sourcePath)
			root, err := uploadDirectoryWithAuth(sourcePath, email, privateKeyPath, proofPath, spaceDID, debug)
			if err != nil {
				log.Fatalf("Failed to upload directory: %v", err)
			}
			log.Printf("✓ Directory uploaded with root CID: %s", root)
			finalCID = root
		} else {
			// Use provided CID directly
			finalCID = cidFlag
			log.Printf("Mounting existing content with CID: %s", finalCID)
		}

		// Create filesystem
		root := fuse.NewStorachaFS(finalCID, debug)

		opts := &gofusefs.Options{
			MountOptions: fusefs.MountOptions{
				FsName: fmt.Sprintf("storachafs-%s", finalCID),
				Name:   "storachafs",
			},
			EntryTimeout: &entryTTL,
			AttrTimeout:  &attrTTL,
		}

		server, err := gofusefs.Mount(mnt, root, opts)
		if err != nil {
			log.Fatalf("mount: %v", err)
		}

		if readOnly {
			log.Printf("✓ Mounted %s at %s (read-only)", finalCID, mnt)
		} else {
			log.Printf("✓ Mounted %s at %s (authenticated - read/write)", finalCID, mnt)
		}
		server.Wait()
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.Flags().DurationVar(&entryTTL, "entry-ttl", time.Second, "kernel dentry TTL")
	mountCmd.Flags().DurationVar(&attrTTL, "attr-ttl", time.Second, "kernel attr TTL")
	mountCmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")

	mountCmd.Flags().StringVar(&cidFlag, "cid", "", "CID of existing Storacha content to mount")
	mountCmd.Flags().StringVar(&sourcePath, "source", "", "local directory path to upload and mount")

	mountCmd.Flags().StringVar(&email, "email", "", "email for email-based authentication")
	mountCmd.Flags().StringVar(&privateKeyPath, "private-key", "", "path to private key file")
	mountCmd.Flags().StringVar(&proofPath, "proof", "", "path to proof/delegation file")
	mountCmd.Flags().StringVar(&spaceDID, "space", "", "space DID to interact with (required for uploads)")
	mountCmd.Flags().BoolVar(&readOnly, "read-only", false, "mount in read-only mode (no authentication)")
}

func uploadDirectoryWithAuth(sourcePath, email, privateKeyPath, proofPath, spaceDID string, debug bool) (string, error) {
	ctx := context.Background()
	if sourcePath == "" {
		return "", fmt.Errorf("source path is required for upload")
	}

	// Parse space DID
	spaceDid, err := did.Parse(spaceDID)
	if err != nil {
		return "", fmt.Errorf("failed to parse space DID: %v", err)
	}
	if debug {
		log.Printf("Using space: %s", spaceDid.String())
	}

	// Create client
	cl, err := client.NewClient()
	if err != nil {
		return "", fmt.Errorf("failed to create client: %v", err)
	}

	// Handle authentication
	if email != "" {
		// Email authentication
		accountDiD, err := didmailto.FromEmail(email)
		if err != nil {
			return "", fmt.Errorf("failed to parse email DID: %v", err)
		}

		if debug {
			log.Printf("Requesting access for %s", accountDiD.String())
		}

		// Request access
		authOk, err := cl.RequestAccess(ctx, accountDiD.String())
		if err != nil {
			return "", fmt.Errorf("failed to request access: %v", err)
		}

		// Poll for user verification
		if debug {
			log.Printf("Waiting for user to verify access request...")
		}
		delegationResult := cl.PollClaim(ctx, authOk)
		r := <-delegationResult

		delegation, err := result.Unwrap(r)
		if err != nil {
			return "", fmt.Errorf("failed to claim access: %v", err)
		}

		if len(delegation) == 0 {
			return "", fmt.Errorf("no delegations received after claiming access")
		}

		if debug {
			log.Printf("Received delegation, size: %d", len(delegation))
		}
		cl.AddProofs(delegation...)

	} else if privateKeyPath != "" && proofPath != "" {
		// TODO: Implement private key authentication
		return "", fmt.Errorf("private key authentication not yet implemented")
	} else {
		return "", fmt.Errorf("authentication required for upload")
	}

	// CAR v2 blockstore approach - create CAR file directly
	if debug {
		log.Printf("🚀 NEW CAR v2 BLOCKSTORE APPROACH - Building CAR file from directory: %s", sourcePath)
	}

	// Walk directory to find all files
	var allFiles []string
	var totalSize int64

	err = filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			allFiles = append(allFiles, path)
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to walk directory: %v", err)
	}

	fileCount := len(allFiles)
	if debug {
		log.Printf("Found %d files, total size: %d bytes", fileCount, totalSize)
	}

	// First, create the content to get the actual root CID
	combinedData := fmt.Sprintf("StorachaFS directory: %s\nFiles:\n", sourcePath)

	for _, filePath := range allFiles {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("failed to read file %s: %v", filePath, err)
		}

		relPath, _ := filepath.Rel(sourcePath, filePath)
		combinedData += fmt.Sprintf("=== %s (%d bytes) ===\n%s\n\n", relPath, len(content), string(content))

		if debug {
			log.Printf("Added file: %s (%d bytes)", relPath, len(content))
		}
	}

	// Create the actual root CID first (force CIDv1 for better compatibility)
	block := blocks.NewBlock([]byte(combinedData))
	// Convert to CIDv1 if it's CIDv0
	originalCID := block.Cid()
	actualRoot := originalCID
	if originalCID.Version() == 0 {
		actualRoot = cid.NewCidV1(originalCID.Type(), originalCID.Hash())
	}

	// Create temporary CAR file with the correct root from the start
	carPath := filepath.Join(os.TempDir(), fmt.Sprintf("storacha_%d.car", time.Now().Unix()))
	defer os.Remove(carPath) // Clean up temp file

	if debug {
		log.Printf("Creating CAR file at: %s", carPath)
	}

	// Create blockstore for CAR v2 with the actual root
	carBlockstore, err := blockstore.OpenReadWrite(carPath, []cid.Cid{actualRoot})
	if err != nil {
		return "", fmt.Errorf("failed to create CAR blockstore: %v", err)
	}

	// Put the block in the CAR
	err = carBlockstore.Put(ctx, block)
	if err != nil {
		carBlockstore.Finalize()
		return "", fmt.Errorf("failed to put block: %v", err)
	}

	// Finalize the CAR file
	if err := carBlockstore.Finalize(); err != nil {
		return "", fmt.Errorf("failed to finalize CAR: %v", err)
	} // Read the CAR file content
	carData, err := os.ReadFile(carPath)
	if err != nil {
		return "", fmt.Errorf("failed to read CAR file: %v", err)
	}

	if debug {
		log.Printf("✅ Created CAR file: %d bytes with root CID: %s", len(carData), actualRoot)
	}

	// Create CID for the CAR file itself (with CAR codec 0x202)
	carCID, err := cid.Prefix{
		Version:  1,
		Codec:    0x202, // CAR codec
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(carData)
	if err != nil {
		return "", fmt.Errorf("failed to create CAR CID: %v", err)
	}

	if debug {
		log.Printf("Uploading to Storacha space: %s", spaceDid.String())
		log.Printf("Content Root CID: %s (codec: 0x%x)", actualRoot, actualRoot.Type())
		log.Printf("CAR Shard CID: %s (codec: 0x%x)", carCID, carCID.Type())
	}

	// Create IPLD links for Storacha - root is the content, shard is the CAR
	rootLink := cidlink.Link{Cid: actualRoot}
	carLink := cidlink.Link{Cid: carCID}

	// Use the CAR CID as the shard (this is what Storacha expects)
	shardLinks := []ipld.Link{carLink}

	if debug {
		log.Printf("Uploading to Storacha...")
		log.Printf("Root Link: %s", rootLink.String())
		log.Printf("Shard Links: %v", shardLinks)
	}

	// Upload to Storacha
	addResult, err := cl.UploadAdd(ctx, spaceDid, rootLink, shardLinks)
	if err != nil {
		if debug {
			log.Printf("Upload failed: %v", err)
			log.Printf("CAR payload was %d bytes", len(carData))
		}
		return "", fmt.Errorf("failed to upload to Storacha: %v", err)
	}

	if debug {
		log.Printf("🎉 Upload successful!")
		log.Printf("Root: %s", addResult.Root)
		log.Printf("Uploaded %d files (%d bytes)", fileCount, totalSize)
	}

	return addResult.Root.String(), nil
}
