// cmd/storachafs/mount.go
package storachafs

import (
	"context"
	"fmt"
	"log"
	"os"

	// "strings"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/auth"
	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/ipld/go-ipld-prime"
	"github.com/spf13/cobra"

	// "github.com/stretchr/testify/require"

	// "github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"

	// "github.com/storacha/go-ucanto/principal"
	// GuppyDelegation "github.com/storacha/guppy/pkg/delegation"
	"github.com/storacha/guppy/pkg/client"

	"database/sql"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/guppy/pkg/didmailto"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"

	_ "modernc.org/sqlite"
)

var (
	entryTTL       time.Duration
	attrTTL        time.Duration
	debug          bool
	email          string
	cid            string
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
		if (cid == "" && sourcePath == "") || (cid != "" && sourcePath != "") {
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
			finalCID = cid
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

	mountCmd.Flags().StringVar(&cid, "cid", "", "CID of existing Storacha content to mount")
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
		return  "", fmt.Errorf("source path is required for upload")
	}

	// parse space
	spaceDid, err := did.Parse(spaceDID)
	if err != nil {
		return "", fmt.Errorf("failed to parse space DID: %v", err)
	} else {
		log.Printf("Using space: %s", spaceDid	.String())
	}
	// var issuer principal.Signer
	// var proofs []delegation.Delegation
	if privateKeyPath != "" && proofPath != "" {
		// todo
	} else if email != "" {
		cl, err := client.NewClient()
		if err != nil {
			return "", fmt.Errorf("failed to create client: %v", err)
		}
		// request access;
		accountDiD, err := didmailto.FromEmail(email	)
		if err != nil {
			return "", fmt.Errorf("failed to parse email DID: %v", err)
		}
		if debug {
			log.Printf("Requesting access for %s", accountDiD.String())
		}

		// request access
		authOk, err := cl.RequestAccess(ctx, accountDiD.String())
		if err != nil {
			return "", fmt.Errorf("failed to request access: %v", err)
		}

		// poll for user verification
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
			log.Printf("Received Delegation, size: %d", len(delegation))
		}
		cl.AddProofs(delegation...)

		// data preparation system setup
		db, err := createDatabase()
		if err != nil {
			return "", fmt.Errorf("failed to create database: %w", err)
		}
		defer db.Close()

		repo := sqlrepo.New(db)
		api := preparation.NewAPI(repo, cl, spaceDid)
		if debug {
			log.Printf("Starting upload of directory: %s", sourcePath)
		}
		
		if err != nil {
			return "", fmt.Errorf("failed to create configuration: %w", err)
		}
		source, err := api.CreateSource(ctx, "temp", sourcePath)
		if err != nil {
			return "", fmt.Errorf("failed to create source: %w", err)
		}
		space, err := api.Spaces.FindOrCreateSpace(ctx, spaceDid, "temp")
		if err != nil {
			return "", fmt.Errorf("failed to create or find space: %w", err)
		}
		err = api.Spaces.Repo.AddSourceToSpace(ctx, space.DID(), source.ID())
		if err != nil {
			return "", fmt.Errorf("failed to add source to space: %w", err)
		}
		if debug {
			log.Printf("Source created with ID: %s", source.ID())
		}
		uploads, err := api.CreateUploads(ctx, space.DID())
		if err != nil {
			return "", fmt.Errorf("failed to create uploads: %w", err)
		}
		if len(uploads) == 0 {
			return "", fmt.Errorf("no uploads created")
		}
		if debug {
			log.Printf("Created %d upload(s)", len(uploads))
		}
		// just uploads[0] for now
		rootCID, err := api.ExecuteUpload(ctx, uploads[0])
		if err != nil {
			return "", fmt.Errorf("failed to execute upload: %w", err)
		}
		if debug {
			log.Printf("Upload completed with root CID: %s", rootCID)
		}
		// Get the shard links that were uploaded  
		shards, err := repo.ShardsForUploadByStatus(ctx, uploads[0].ID(), model.ShardStateAdded)
		if err != nil {
			return "", fmt.Errorf("failed to get shards for upload: %w", err)
		}
		if len(shards) == 0 {
			return "", fmt.Errorf("no shards found for upload")
		}
		if debug {
			log.Printf("Found %d shard(s) for upload", len(shards))
		}  
		var shardLinks []ipld.Link
		// TODO : how to get the shardlinks??
		for _, shard := range shards {
			shardLinks = append(shardLinks, )
		}
		rootLink := cidlink.Link{Cid: rootCID}
		// Register the upload  
		addOk, err := cl.UploadAdd(ctx, space.DID(), rootLink, shardLinks)
		if err != nil {
			return "", fmt.Errorf("failed to register upload: %w", err)
		}
		if debug {
			log.Printf("Upload registered successfully: %+v", addOk)
		}
		return addOk.Root.String(), nil
	} else {
		return "", fmt.Errorf("unsupported authentication method")
	}
	return "temp", nil

}

func createDatabase() (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Initialize schema
	_, err = db.Exec(sqlrepo.Schema)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute schema: %w", err)
	}

	_, err = db.Exec("PRAGMA foreign_keys = OFF;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	return db, nil
}


	