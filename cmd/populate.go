package cmd

import (
	"time"

	"github.com/fabito/azure-storage-purger/pkg/test"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var maxNumberOfEntitiesPerPartition int
var startYear int

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Add dummy data to Azure Storage Table",
	Long:  `This is used for testing the purge command`,
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Date(startYear, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Now().UTC()

		if start.After(end) {
			log.Fatalf("Start year %s cannot be in the future")
		}

		err := test.PopulateTable(accountName, accountKey, tableName, start, end, maxNumberOfEntitiesPerPartition)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	tableCmd.AddCommand(populateCmd)

	populateCmd.Flags().IntVar(&maxNumberOfEntitiesPerPartition, "max-num-entities", 1, "Number of entities per partition")
	populateCmd.MarkFlagRequired("max-num-entities")

	populateCmd.Flags().IntVar(&startYear, "start-year", 2018, "Star year for data generation")

}
