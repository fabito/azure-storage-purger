package cmd

import (
	"os"

	"github.com/fabito/azure-storage-purger/pkg/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var maxNumberOfEntitiesPerPartition int

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Add dummy data to Azure Storage Table",
	Long:  `This is used for testing the purge command`,
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Info("Starting populate")
		err := test.PopulateTable(accountName, accountKey, tableName, maxNumberOfEntitiesPerPartition)
		if err != nil {
			logrus.Fatal(err)
			os.Exit(1)
		}
	},
}

func init() {
	tableCmd.AddCommand(populateCmd)

	populateCmd.Flags().IntVar(&maxNumberOfEntitiesPerPartition, "max-num-entities", 1, "Number of entities per partition")
	populateCmd.MarkFlagRequired("max-num-entities")
}
