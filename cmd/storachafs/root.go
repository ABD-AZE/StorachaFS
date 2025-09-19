// cmd/storachafs/root.go
package storachafs

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "storachafs",
	Short: "A FUSE filesystem for Storacha.",
	Long: `StorachaFS is a Go-based FUSE filesystem that mounts Storacha
spaces as POSIX-like directories, allowing seamless read/write access
to files stored on the decentralized Storacha network.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %v\n", err)
		os.Exit(1)
	}
}
