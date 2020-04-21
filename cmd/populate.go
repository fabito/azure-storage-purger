package cmd

import (
	"log"
	"os"

	"github.com/fabito/azure-storage-purger/pkg/test"
	"github.com/spf13/cobra"
)

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Add dummy data to Azure Storage Table",
	Long:  `This is used for testing the purge command`,
	Run: func(cmd *cobra.Command, args []string) {
		err := test.PopulateTable(accountName, accountKey, tableName)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
	},
}

func init() {
	tableCmd.AddCommand(populateCmd)
}
