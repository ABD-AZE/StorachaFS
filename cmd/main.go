package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ABD-AZE/StorachaFS/internal/auth"
	"github.com/ABD-AZE/StorachaFS/internal/fuse"
	pkauth "github.com/ABD-AZE/StorachaFS/internal/pk-auth"
	"github.com/hanwen/go-fuse/v2/fs"
	fusefs "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/storacha/guppy/pkg/client"
)

func main() {
	log.SetFlags(0)
	// if len(os.Args) < 3 {
	// 	usage()
	// }
	mountCmd(os.Args[1:])
}

func usage() {
	// usage message
	fmt.Fprintf(os.Stderr, `Usage:
  storachafs <CID> <mountpoint> [options]

Authentication Options:
  --email EMAIL                Email for email-based authentication
  --private-key PATH           Path to private key file
  --proof PATH                 Path to proof/delegation file  
  --space DID                  Space DID to interact with
  --read-only                  Mount in read-only mode (no authentication)

FUSE Options:
  --entry-ttl DURATION         Kernel dentry TTL (default: 1s)
  --attr-ttl DURATION          Kernel attr TTL (default: 1s)
  --debug                      Enable debug logging

Environment Variables (for private key auth):
  STORACHA_PRIVATE_KEY_PATH    Path to private key file
  STORACHA_PROOF_PATH          Path to proof file
  STORACHA_SPACE_DID           Space DID

Examples:
  # Read-only mount (no auth required)
  storachafs QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG /mnt/storacha --read-only

  # Email authentication
  storachafs QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG /mnt/storacha --email user@example.com

  # Private key authentication
  storachafs QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG /mnt/storacha \
    --private-key ./envs/storacha/private.key \
    --proof ./envs/storacha/proof.ucan \
    --space did:key:z6MkkntK8FVXXL3bXYZU9LstfydCU8qY6m8CEheZt1PGdYKP

  # Using environment variables
  export STORACHA_PRIVATE_KEY_PATH=./envs/storacha/private.key
  export STORACHA_PROOF_PATH=./envs/storacha/proof.ucan
  export STORACHA_SPACE_DID=did:key:z6MkkntK8FVXXL3bXYZU9LstfydCU8qY6m8CEheZt1PGdYKP
  storachafs QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG /mnt/storacha
`)
	os.Exit(2)
}

func mountCmd(args []string) {
	flagsWithValue := map[string]bool{
		"--entry-ttl":   true,
		"--attr-ttl":    true,
		"-entry-ttl":    true,
		"-attr-ttl":     true,
		"--email":       true,
		"--private-key": true,
		"--proof":       true,
		"--space":       true,
		"-email":        true,
		"-private-key":  true,
		"-proof":        true,
		"-space":        true,
	}

	var flagArgs []string
	var posArgs []string

	for i := 0; i < len(args); i++ {
		a := args[i]
		if len(a) > 0 && a[0] == '-' {
			flagArgs = append(flagArgs, a)

			if flagsWithValue[a] && i+1 < len(args) {
				next := args[i+1]
				if len(next) > 0 && next[0] != '-' {
					flagArgs = append(flagArgs, next)
					i++ // consume value
				}
			}
		} else {
			posArgs = append(posArgs, a)
		}
	}

	reordered := append(flagArgs, posArgs...)

	fsFlags := flag.NewFlagSet("mount", flag.ExitOnError)

	// FUSE options
	entryTTL := fsFlags.Duration("entry-ttl", time.Second, "kernel dentry TTL")
	attrTTL := fsFlags.Duration("attr-ttl", time.Second, "kernel attr TTL")
	debug := fsFlags.Bool("debug", false, "enable debug logging")

	// Authentication options
	email := fsFlags.String("email", "", "email for email-based authentication")
	privateKeyPath := fsFlags.String("private-key", "", "path to private key file")
	proofPath := fsFlags.String("proof", "", "path to proof/delegation file")
	spaceDID := fsFlags.String("space", "", "space DID to interact with")
	readOnly := fsFlags.Bool("read-only", false, "mount in read-only mode")

	// Add help flag
	help := fsFlags.Bool("help", false, "show help message")
	fsFlags.BoolVar(help, "h", false, "show help message")

	// Now parse the reordered args
	if err := fsFlags.Parse(reordered); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		usage()
		return
	}

	// Show help if requested
	if *help {
		usage()
		return
	}

	// After parsing flags, the remaining args (positional) are fsFlags.Args()
	if fsFlags.NArg() != 2 {
		fmt.Printf("Error: Expected exactly 2 arguments (CID and mountpoint), got %d\n", fsFlags.NArg())
		fmt.Printf("Arguments provided: %v\n", fsFlags.Args())
		usage()
		return
	}

	cid := fsFlags.Arg(0)
	mnt := fsFlags.Arg(1)

	// Create mount point if it doesn't exist
	if err := os.MkdirAll(mnt, 0755); err != nil {
		log.Fatalf("Failed to create mount point %s: %v", mnt, err)
	}

	var storachaClient *client.Client

	// Determine authentication method
	if *readOnly {
		log.Println("Mounting in read-only mode (no authentication)")
		storachaClient = nil
	} else {
		authMethod, err := pkauth.GetAuthMethodFromArgs(*email, *privateKeyPath, *proofPath, *spaceDID)
		if err != nil {
			log.Fatalf("Authentication error: %v", err)
		}

		switch authMethod {
		case "email":
			log.Println("Using email authentication...")
			storachaClient, err = auth.EmailAuth(*email)
			if err != nil {
				log.Fatalf("Email authentication failed: %v", err)
			}
			log.Println("✓ Successfully authenticated via email")

		case "private_key":
			log.Println("Using private key authentication...")
			var authConfig *pkauth.AuthConfig

			if *privateKeyPath != "" && *proofPath != "" && *spaceDID != "" {
				authConfig = pkauth.LoadAuthConfigFromFlags(*privateKeyPath, *proofPath, *spaceDID)
			} else {
				authConfig, err = pkauth.LoadAuthConfigFromEnv()
				if err != nil {
					log.Fatalf("Private key authentication failed: %v", err)
				}
			}

			if err := pkauth.ValidateAuthConfig(authConfig); err != nil {
				log.Fatalf("Authentication validation failed: %v", err)
			}

			storachaClient, err = pkauth.PrivateKeyAuth(authConfig)
			if err != nil {
				log.Fatalf("Private key authentication failed: %v", err)
			}
			log.Println("✓ Successfully authenticated via private key")

		case "none":
			log.Println("No authentication provided - mounting in read-only mode")
			log.Println("For write operations, provide authentication via:")
			log.Println("  --email for email auth, or")
			log.Println("  --private-key, --proof, --space for private key auth")
			storachaClient = nil
		}
	}

	// Create filesystem
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

	if storachaClient != nil {
		log.Printf("✓ Mounted %s at %s (authenticated - read/write)", cid, mnt)
	} else {
		log.Printf("✓ Mounted %s at %s (read-only)", cid, mnt)
	}

	server.Wait()
}
