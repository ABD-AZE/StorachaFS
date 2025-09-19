// cmd/storachafs/status.go
package storachafs

import (
	"fmt"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show local vs remote changes",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("status command not yet implemented")
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
