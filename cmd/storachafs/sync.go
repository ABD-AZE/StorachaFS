// cmd/storachafs/sync.go
package storachafs

import (
	"fmt"

	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync local files to Storacha",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("sync command not yet implemented")
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
}
