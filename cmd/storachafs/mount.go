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
	"bytes"
	"io"
	"path"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
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

	// Build CAR from directory
	carData, rootLink, shardLinks, fileCount, totalSize, err := CreateCar(sourcePath)
	if err != nil {
		return "", fmt.Errorf("failed to create CAR: %v", err)
	}
    
	// save as file for debug
	if debug {
		debugCarPath := path.Join(os.TempDir(), "storacha-upload.car")
		err := os.WriteFile(debugCarPath, carData, 0644)
		if err != nil {
			log.Printf("Failed to write debug CAR file: %v", err)
		} else {
			log.Printf("Debug CAR file written to: %s", debugCarPath)
		}
	}
	rL := cidlink.Link{Cid: cid.MustParse(rootLink)}
	shardLinksCid := make([]ipld.Link, len(shardLinks))
	for i, link := range shardLinks {
		shardLinksCid[i] = cidlink.Link{Cid: cid.MustParse(link)}
	}

	// Upload to Storacha (pass CAR payload and links)
	addResult, err := cl.UploadAdd(ctx, spaceDid, rL, shardLinksCid)
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

// (old stub removed)

// CreateCar builds a CAR file from the provided sourcePath directory.
// Returns: car bytes, root link (cid string), shardLinks (slice of cid strings), fileCount, totalBytes, error
func CreateCar(sourcePath string) ([]byte, string, []string, int, int64, error) {
	if sourcePath == "" {
		return nil, "", nil, 0, 0, fmt.Errorf("sourcePath required")
	}

	// We'll use a temporary ReadWrite blockstore backed by the in-memory CAR writer
	// The blockstore OpenReadWrite requires a path; use an in-memory variant by using blockstore.NewReadWrite? Use the CAR library to write directly.
	// Use blockstore.OpenReadWrite to create a ReadWrite that writes to a file path. To avoid files, write to a temp file then read it back.
	tmpFile, err := os.CreateTemp("", "storacha-*.car")
	if err != nil {
		return nil, "", nil, 0, 0, fmt.Errorf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// make a fake proxy root to initialize the car
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return nil, "", nil, 0, 0, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return nil, "", nil, 0, 0, err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagPb), hash)

	options := []car.Option{blockstore.WriteAsCarV1(true)}
	rdw, err := blockstore.OpenReadWrite(tmpPath, []cid.Cid{proxyRoot}, options...)
	if err != nil {
		return nil, "", nil, 0, 0, fmt.Errorf("open readwrite car failed: %v", err)
	}

	// Build unixfs files into the blockstore and get root CID
	rootCid, err := writeFiles(context.Background(), false, rdw, sourcePath)
	if err != nil {
		return nil, "", nil, 0, 0, fmt.Errorf("writing files into blockstore failed: %v", err)
	}

	if err := rdw.Finalize(); err != nil {
		return nil, "", nil, 0, 0, fmt.Errorf("finalize car failed: %v", err)
	}

	// read CAR bytes from temp file
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, "", nil, 0, 0, fmt.Errorf("reading car file failed: %v", err)
	}

	// For now, shardLinks is empty - guppy UploadAdd should be able to accept full CAR and root
	shardLinks := []string{}

	// Count files and total size (walk)
	var fileCount int
	var totalSize int64
	err = filepath.Walk(sourcePath, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		return nil, "", nil, 0, 0, err
	}

	return data, rootCid.String(), shardLinks, fileCount, totalSize, nil
}

func writeFiles(ctx context.Context, noWrap bool, bs *blockstore.ReadWrite, paths ...string) (cid.Cid, error) {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := bs.Get(ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}
	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			bs.Put(ctx, blk)
			return nil
		}, nil
	}

	topLevel := make([]dagpb.PBLink, 0, len(paths))
	for _, p := range paths {
		l, size, err := builder.BuildUnixFSRecursive(p, &ls)
		if err != nil {
			return cid.Undef, err
		}
		if noWrap {
			rcl, ok := l.(cidlink.Link)
			if !ok {
				return cid.Undef, fmt.Errorf("could not interpret %s", l)
			}
			return rcl.Cid, nil
		}
		name := path.Base(p)
		entry, err := builder.BuildUnixFSDirectoryEntry(name, int64(size), l)
		if err != nil {
			return cid.Undef, err
		}
		topLevel = append(topLevel, entry)
	}

	// make a directory for the file(s).

	root, _, err := builder.BuildUnixFSDirectory(topLevel, &ls)
	if err != nil {
		return cid.Undef, nil
	}
	rcl, ok := root.(cidlink.Link)
	if !ok {
		return cid.Undef, fmt.Errorf("could not interpret %s", root)
	}

	return rcl.Cid, nil
}
