// cmd/storachafs/pull.go
package storachafs

import (
	"fmt"

	"github.com/spf13/cobra"
)

var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Fetch new or updated files from Storacha",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("pull command not yet implemented")
	},
}

func init() {
	rootCmd.AddCommand(pullCmd)
}
