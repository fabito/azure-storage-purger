package cmd

import (
	"os"

	"github.com/fabito/azure-storage-purger/pkg/purger"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	purgeEntitiesOlderThanDays int
	periodLengthInHours        int
	dryRun                     bool
	usePool                    bool
)

// purgeCmd represents the purge command
var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purges entities older than purgeEntitiesOlderThanDays",
	Long:  `Purges entities older than purgeEntitiesOlderThanDays`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("Starting purge")

		purger, err := purger.NewTablePurger(accountName, accountKey, tableName, purgeEntitiesOlderThanDays, periodLengthInHours, numWorkers, usePool, dryRun)
		if err != nil {
			log.Fatal(err)
		}

		result, err := purger.PurgeEntities()
		if err != nil {
			log.Fatal(err)
		}

		if result.HasErrors() {
			os.Exit(1)
		}
	},
}

func init() {
	tableCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().IntVar(&purgeEntitiesOlderThanDays, "num-days-to-keep", 365, "Number of days to keep")
	purgeCmd.Flags().IntVar(&periodLengthInHours, "num-hours-per-worker", 24, "Number of hours per worker")

	purgeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
	purgeCmd.Flags().BoolVar(&usePool, "use-pool", false, "Enable worker pool mode")
}
