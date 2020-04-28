package cmd

import (
	"time"

	"github.com/fabito/azure-storage-purger/pkg/populator"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	maxNumberOfEntitiesPerPartition int
	startYear                       int
)

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Create and add dummy data to an Azure Storage Tablefor testing purposes",
	Long:  `This is used for testing the purge command`,
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Date(startYear, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Now().UTC()

		if start.After(end) {
			log.Fatalf("Start %s cannot be in the future", start)
		}

		err := populator.PopulateTable(accountName, accountKey, tableName, start, end, maxNumberOfEntitiesPerPartition, numWorkers)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	tableCmd.AddCommand(populateCmd)
	populateCmd.Flags().IntVar(&maxNumberOfEntitiesPerPartition, "max-num-entities", 5000, "Number of entities per partition")
	populateCmd.Flags().IntVar(&startYear, "start-year", 2018, "Star year for data generation")
}
