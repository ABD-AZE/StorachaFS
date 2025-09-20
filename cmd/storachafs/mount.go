// cmd/storachafs/mount.go
package storachafs

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/auth"
	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	"github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"

	// UCAN / DID / signer
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	guppyDelegation "github.com/storacha/guppy/pkg/delegation"

	// Guppy client
	"github.com/storacha/guppy/pkg/client"

	// CAR + CID + multihash
	ipfscid "github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/multiformats/go-multihash"
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

	mountCmd.Flags().StringVar(&cid, "cid", "", "CID of existing Storacha content to mount")
	mountCmd.Flags().StringVar(&sourcePath, "source", "", "local directory path to upload and mount")

	mountCmd.Flags().StringVar(&email, "email", "", "email for email-based authentication")
	mountCmd.Flags().StringVar(&privateKeyPath, "private-key", "", "path to private key file")
	mountCmd.Flags().StringVar(&proofPath, "proof", "", "path to proof/delegation file")
	mountCmd.Flags().StringVar(&spaceDID, "space", "", "space DID to interact with (required for uploads)")
	mountCmd.Flags().BoolVar(&readOnly, "read-only", false, "mount in read-only mode (no authentication)")
}

// uploadDirectoryWithAuth: pack dir to CAR, store shard with StoreAdd, then UploadAdd to register upload.
// Accepts either email interactive auth (requires user to authenticate via RequestAccess/CLI flow) OR
// requires private-key + proof for non-interactive auth. For simplicity this implementation prefers private-key+proof.
func uploadDirectoryWithAuth(localPath, emailArg, privateKeyPathArg, proofPathArg, spaceDIDStr string, debug bool) (string, error) {
	ctx := context.Background()

	// validate
	if localPath == "" {
		return "", fmt.Errorf("localPath required")
	}

	// parse space
	space, err := did.Parse(spaceDIDStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse space DID '%s': %v", spaceDIDStr, err)
	}

	// load signer + proofs (preferred path for programmatic upload)
	var issuer principal.Signer
	var proofs []delegation.Delegation
	if privateKeyPathArg != "" && proofPathArg != "" {
		privBytes, err := os.ReadFile(privateKeyPathArg)
		if err != nil {
			return "", fmt.Errorf("read private key: %v", err)
		}
		issuer, err = signer.Parse(strings.TrimSpace(string(privBytes)))
		if err != nil {
			return "", fmt.Errorf("parse private key: %v", err)
		}

		prfBytes, err := os.ReadFile(proofPathArg)
		if err != nil {
			return "", fmt.Errorf("read proof file: %v", err)
		}
		proof, err := guppyDelegation.ExtractProof(prfBytes)
		if err != nil {
			return "", fmt.Errorf("extract proofs: %v", err)
		}
		proofs = []delegation.Delegation{proof}
	} else if emailArg != "" {
		// Interactive flow: use email auth but we need issuer & proofs for the upload API
		if debug {
			log.Printf("Using email authentication for upload (interactive)")
		}
		guppyClient, err := auth.EmailAuth(emailArg)
		if err != nil {
			return "", fmt.Errorf("email auth failed: %v", err)
		}

		// For email auth, we need to extract the issuer and proofs from the authenticated client
		// This is a workaround since the upload APIs expect issuer + proofs directly
		// In practice, you might want to use the client's upload methods directly if available

		// Pack directory to CAR (shell out)
		carPath, err := packDirectoryToCAR(localPath, debug)
		if err != nil {
			return "", fmt.Errorf("packDirectoryToCAR: %v", err)
		}
		defer func() { _ = os.Remove(carPath) }()

		// Get root CID from CAR
		rootCid, err := carRootCIDFromFile(carPath)
		if err != nil {
			return "", fmt.Errorf("get car root cid: %v", err)
		}

		// Use the authenticated guppy client to upload the CAR file directly
		carFile, err := os.Open(carPath)
		if err != nil {
			return "", fmt.Errorf("open car file: %v", err)
		}
		defer func() { _ = carFile.Close() }()

		// Use SpaceBlobAdd method for email-authenticated uploads
		// NOTE: SpaceBlobAdd uploads the CAR file to the space and returns the space's CID for this content
		blobCid, _, err := guppyClient.SpaceBlobAdd(ctx, carFile, space)
		if err != nil {
			return "", fmt.Errorf("SpaceBlobAdd failed: %v", err)
		}

		if debug {
			log.Printf("Email-based upload completed. UnixFS root: %s, Space blob CID: %s", rootCid.String(), blobCid.String())
		}

		// CRITICAL: Return the space's blob CID, not the UnixFS root CID
		return blobCid.String(), nil
	} else {
		return "", fmt.Errorf("provide --private-key and --proof for programmatic upload")
	}

	// pack directory to CAR (shell out)
	carPath, err := packDirectoryToCAR(localPath, debug)
	if err != nil {
		return "", fmt.Errorf("packDirectoryToCAR: %v", err)
	}
	// make sure temp file cleaned up
	defer func() { _ = os.Remove(carPath) }()

	// read CAR bytes (docs example uses full bytes; for large CARs prefer streaming)
	carBytes, err := os.ReadFile(carPath)
	if err != nil {
		return "", fmt.Errorf("read car file: %v", err)
	}

	// compute CAR multihash (sha2-256)
	mh, err := multihash.Sum(carBytes, multihash.SHA2_256, -1)
	if err != nil {
		return "", fmt.Errorf("multihash sum: %v", err)
	}
	// build CAR shard CID (cidv1, codec 0x0202 per docs)
	shardCid := ipfscid.NewCidV1(0x0202, mh) // 0x0202 = CAR codec

	// read root CID(s) from CAR using go-car
	rootCid, err := carRootCIDFromFile(carPath)
	if err != nil {
		return "", fmt.Errorf("get car root cid: %v", err)
	}

	if debug {
		log.Printf("CAR root: %s ; shard CID: %s ; size: %d bytes", rootCid.String(), shardCid.String(), len(carBytes))
	}

	// Create a Guppy client with the issuer and proofs
	guppyClient, err := client.NewClient(client.WithPrincipal(issuer))
	if err != nil {
		return "", fmt.Errorf("failed to create guppy client: %v", err)
	}

	// Add proofs to the client
	if err := guppyClient.AddProofs(proofs...); err != nil {
		return "", fmt.Errorf("failed to add proofs to client: %v", err)
	}

	// Use SpaceBlobAdd to upload the CAR file
	carFile, err := os.Open(carPath)
	if err != nil {
		return "", fmt.Errorf("failed to open CAR file: %v", err)
	}
	defer func() { _ = carFile.Close() }()

	// Upload the CAR using SpaceBlobAdd
	// NOTE: SpaceBlobAdd uploads the CAR file to the space and returns the space's CID for this content
	blobMultihash, _, err := guppyClient.SpaceBlobAdd(ctx, carFile, space)
	if err != nil {
		return "", fmt.Errorf("SpaceBlobAdd failed: %v", err)
	}

	actualCid := ipfscid.NewCidV1(0x55, blobMultihash) // 0x55 = raw codec
	if debug {
		log.Printf("CAR uploaded to space. UnixFS root: %s, Space blob CID: %s", rootCid.String(), actualCid.String())
	}
	return actualCid.String(), nil
	// CRITICAL: Return the space's blob CID, not the UnixFS root CID
	// The space blob CID is what exists in the space DAG and can be mounted
	// The UnixFS root CID is internal to the CAR file and not directly accessible
}

// packDirectoryToCAR: tries `npx ipfs-car pack <dir> --output <tmp>` first, falls back to `car create` (go-car) for directory packing
// Returns path to temp CAR file.
func packDirectoryToCAR(srcDir string, debug bool) (string, error) {
	// ensure directory
	info, err := os.Stat(srcDir)
	if err != nil {
		return "", fmt.Errorf("stat source dir: %v", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("source not a directory")
	}

	tmp, err := os.CreateTemp("", "storacha-*.car")
	if err != nil {
		return "", fmt.Errorf("create tmp file: %v", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp file: %v", err)
	}

	// try ipfs-car (Node.js tool) - primary choice for directories
	if path, e := exec.LookPath("npx"); e == nil && path != "" {
		cmd := exec.Command("npx", "ipfs-car", "pack", srcDir, "--output", tmpPath)
		if debug {
			log.Printf("Running: %s", strings.Join(cmd.Args, " "))
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			if debug {
				log.Printf("ipfs-car output: %s", string(out))
			}
			return tmpPath, nil
		}
		if debug {
			log.Printf("ipfs-car failed: %v out=%s", err, string(out))
		}
	}

	// fallback: try `car create` (go-car)
	if path, e := exec.LookPath("car"); e == nil && path != "" {
		cmd := exec.Command("car", "create", "-o", tmpPath, srcDir)
		if debug {
			log.Printf("Running: %s", strings.Join(cmd.Args, " "))
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			if debug {
				log.Printf("car create output: %s", string(out))
			}
			return tmpPath, nil
		}
		if debug {
			log.Printf("car create failed: %v out=%s", err, string(out))
		}
	}

	// try ipfs-car (npm) - alternative if npx not available
	if path, e := exec.LookPath("ipfs-car"); e == nil && path != "" {
		// ipfs-car usage: ipfs-car --pack <dir> --output <out>
		cmd := exec.Command("ipfs-car", "--pack", srcDir, "--output", tmpPath)
		if debug {
			log.Printf("Running: %s", strings.Join(cmd.Args, " "))
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			if debug {
				log.Printf("ipfs-car failed: %v out=%s", err, string(out))
			}
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("ipfs-car pack failed: %v: %s", err, string(out))
		}
		if debug {
			log.Printf("ipfs-car output: %s", string(out))
		}
		return tmpPath, nil
	}

	_ = os.Remove(tmpPath)
	return "", fmt.Errorf("no CAR packer found on PATH; install go-car (car) or ipfs-car (npm)")
}

// carRootCIDFromFile uses go-car to open the CAR and returns the first root CID (ipfs/go-cid)
func carRootCIDFromFile(carPath string) (ipfscid.Cid, error) {
	f, err := os.Open(carPath)
	if err != nil {
		return ipfscid.Cid{}, err
	}
	defer func() { _ = f.Close() }()

	r, err := carv2.NewReader(f)
	if err != nil {
		return ipfscid.Cid{}, fmt.Errorf("open car: %v", err)
	}
	roots, err := r.Roots()
	if err != nil {
		return ipfscid.Cid{}, fmt.Errorf("failed to get roots: %v", err)
	}
	if len(roots) == 0 {
		return ipfscid.Cid{}, fmt.Errorf("car has no roots")
	}
	root := roots[0]
	return root, nil
}
