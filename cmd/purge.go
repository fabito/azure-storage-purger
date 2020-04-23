package cmd

import (
	"os"
	"runtime"

	"github.com/fabito/azure-storage-purger/pkg/purger"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	numWorkers                 int
	purgeEntitiesOlderThanDays int
	periodLengthInDays         int
	dryRun                     bool
)

// purgeCmd represents the purge command
var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purges entities older than purgeEntitiesOlderThanDays",
	Long:  `Purges entities older than purgeEntitiesOlderThanDays`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("Starting purge")

		purger, err := purger.NewTablePurger(accountName, accountKey, tableName, purgeEntitiesOlderThanDays, periodLengthInDays, numWorkers, dryRun)
		if err != nil {
			log.Fatal(err)
		}

		result, err := purger.PurgeEntities()
		if err != nil {
			log.Fatal(err)
		}
		log.Debugf("%#v", result)
		log.Infof("It took %s", result.EndTime.Sub(result.StartTime))
		log.Infof("To delete %d entities in %d batches", result.RowCount, result.BatchCount)
		log.Infof("Errors in %d batches (%d entities)", result.BatchErrorCount, result.RowErrorCount)
		log.Debugf("Total requested %d page(s)", result.PageCount)

		if result.HasErrors() {
			os.Exit(1)
		}
	},
}

func init() {
	tableCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().IntVar(&purgeEntitiesOlderThanDays, "num-days-to-keep", 365, "Number of days to keep")
	purgeCmd.Flags().IntVar(&periodLengthInDays, "num-days-per-worker", 90, "Number of days per worker")
	purgeCmd.Flags().IntVar(&numWorkers, "num-workers", runtime.NumCPU()*2, "Number of workers. Default is cpus * 2")
	purgeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
}
