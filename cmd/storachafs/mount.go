// cmd/storachafs/mount.go
package storachafs

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/auth"
	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	"github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	_ "modernc.org/sqlite"
)

var (
	entryTTL       time.Duration
	attrTTL        time.Duration
	debug          bool
	email          string
	uploadPath     string
	privateKeyPath string
	proofPath      string
	spaceDID       string
	readOnly       bool
)

var mountCmd = &cobra.Command{
	Use:   "mount [CID_or_LOCAL_PATH] [mountpoint]",
	Short: "Mount a Storacha space or upload and mount a local directory",
	Long: `Mount a Storacha space by CID, or upload a local directory to Storacha and mount it.

Authentication Options:
  Use --email for email-based authentication, or
  Use --private-key, --proof, --space for private key authentication, or
  Use --read-only for read-only access (no authentication)

Environment Variables (for private key auth):
  STORACHA_PRIVATE_KEY_PATH    Path to private key file
  STORACHA_PROOF_PATH          Path to proof file
  STORACHA_SPACE_DID           Space DID

Examples:
  # Mount existing Storacha content by CID (email auth)
  storachafs mount bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi /mnt/storacha --email user@example.com
  
  # Upload local directory and mount it (email auth)
  storachafs mount /path/to/local/folder /mnt/storacha --email user@example.com --upload
  
  # Mount with private key authentication
  storachafs mount bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi /mnt/storacha \
    --private-key ./envs/storacha/private.key \
    --proof ./envs/storacha/proof.ucan \
    --space did:key:z6MkkntK8FVXXL3bXYZU9LstfydCU8qY6m8CEheZt1PGdYKP
  
  # Read-only mount (no authentication)
  storachafs mount bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi /mnt/storacha --read-only`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		cidOrPath := args[0]
		mnt := args[1]

		// Create mount point if it doesn't exist
		if err := os.MkdirAll(mnt, 0755); err != nil {
			log.Fatalf("Failed to create mount point %s: %v", mnt, err)
		}

		var finalCID string
		var authEmail string

		// Determine authentication method and validate
		if !readOnly {
			authMethod, err := auth.GetAuthMethodFromArgs(email, privateKeyPath, proofPath, spaceDID)
			if err != nil {
				log.Fatalf("Authentication error: %v", err)
			}

			switch authMethod {
			case "email":
				log.Println("Using email authentication...")
				authEmail = email
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
				// For private key auth, we'll use a placeholder email for the filesystem
				authEmail = "private-key-auth"
			case "none":
				log.Println("No authentication provided - mounting in read-only mode")
				log.Println("For write operations, provide authentication via:")
				log.Println("  --email for email auth, or")
				log.Println("  --private-key, --proof, --space for private key auth")
				readOnly = true
			}
		} else {
			log.Println("Mounting in read-only mode (no authentication)")
		}

		// Check if first argument is a CID or a local path
		if uploadPath != "" || !isCID(cidOrPath) {
			// Upload directory first
			localPath := cidOrPath
			if uploadPath != "" {
				localPath = uploadPath
			}

			if readOnly {
				log.Fatalf("Cannot upload directory in read-only mode. Please provide authentication.")
			}

			log.Printf("Uploading directory: %s", localPath)
			uploadedCID, err := uploadDirectory(localPath, authEmail, debug)
			if err != nil {
				log.Fatalf("Failed to upload directory: %v", err)
			}
			log.Printf("✓ Directory uploaded with CID: %s", uploadedCID)
			finalCID = uploadedCID
		} else {
			// Use provided CID directly
			finalCID = cidOrPath
		}

		// Create filesystem
		var root *fuse.StorachaFS
		if readOnly {
			root = fuse.NewStorachaFS(finalCID, debug)
		} else {
			root = fuse.NewStorachaFS(finalCID, debug)
		}

		opts := &fs.Options{
			MountOptions: fusefs.MountOptions{
				FsName: fmt.Sprintf("storachafs-%s", finalCID),
				Name:   "storachafs",
			},
			EntryTimeout: &entryTTL,
			AttrTimeout:  &attrTTL,
		}

		server, err := fs.Mount(mnt, root, opts)
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

	mountCmd.Flags().StringVar(&email, "email", "", "email for email-based authentication")
	mountCmd.Flags().StringVar(&privateKeyPath, "private-key", "", "path to private key file")
	mountCmd.Flags().StringVar(&proofPath, "proof", "", "path to proof/delegation file")
	mountCmd.Flags().StringVar(&spaceDID, "space", "", "space DID to interact with")
	mountCmd.Flags().BoolVar(&readOnly, "read-only", false, "mount in read-only mode (no authentication)")

	mountCmd.Flags().StringVar(&uploadPath, "upload", "", "upload local directory to Storacha before mounting (alternative to specifying local path as first argument)")
}

// isCID checks if a string looks like a valid CID
func isCID(s string) bool {
	// Simple heuristic: CIDs typically start with "Qm" (v0) or "bafy" (v1) and are long
	return (strings.HasPrefix(s, "Qm") && len(s) == 46) ||
		(strings.HasPrefix(s, "bafy") && len(s) > 50) ||
		(strings.HasPrefix(s, "bafk") && len(s) > 50)
}

// uploadDirectory uploads a local directory to Storacha and returns the root CID
func uploadDirectory(localPath, email string, debug bool) (string, error) {
	ctx := context.Background()

	// Get authenticated Guppy client
	guppyClient, err := auth.EmailAuth(email)
	if err != nil {
		return "", fmt.Errorf("failed to authenticate: %v", err)
	}

	// Use placeholder space DID (in production, this should be configurable)
	spaceDID, err := did.Parse("did:key:z6MkkntK8FVXXL3bXYZU9LstfydCU8qY6m8CEheZt1PGdYKP")
	if err != nil {
		return "", fmt.Errorf("failed to parse space DID: %v", err)
	}

	// Create a temporary in-memory database for the preparation API
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return "", fmt.Errorf("failed to create in-memory database: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Warning: failed to close database: %v", closeErr)
		}
	}()

	// Initialize the database schema
	if _, err := db.Exec(sqlrepo.Schema); err != nil {
		return "", fmt.Errorf("failed to initialize database schema: %v", err)
	}

	// Create the repository
	repo := sqlrepo.New(db)

	// Create the preparation API
	prepAPI := preparation.NewAPI(repo, guppyClient, spaceDID)

	// Get directory name for the source
	dirName := filepath.Base(localPath)
	if debug {
		log.Printf("Creating source for directory: %s", dirName)
	}

	// Create source from local directory
	source, err := prepAPI.CreateSource(ctx, dirName, localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create source: %v", err)
	}

	if debug {
		log.Printf("Created source: %s", source.Name())
	}

	// Create uploads for the source
	uploads, err := prepAPI.CreateUploads(ctx, spaceDID)
	if err != nil {
		return "", fmt.Errorf("failed to create uploads: %v", err)
	}

	if debug {
		log.Printf("Created %d uploads", len(uploads))
	}

	// Execute the upload to get the final root CID
	rootCID, err := prepAPI.ExecuteUpload(ctx, uploads[0])
	if err != nil {
		return "", fmt.Errorf("failed to execute upload: %v", err)
	}

	if debug {
		log.Printf("Upload completed with root CID: %s", rootCID.String())
	}

	return rootCID.String(), nil
}
