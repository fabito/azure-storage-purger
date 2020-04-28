package cmd

import (
	"os"

	"github.com/fabito/azure-storage-purger/pkg/purger"
	"github.com/fabito/azure-storage-purger/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	purgeEntitiesOlderThanDays int
	periodLengthInHours        int
	dryRun                     bool
	usePool                    bool
	startDate                  string
	endDate                    string
)

// purgeCmd represents the purge command
var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purges entities older than purgeEntitiesOlderThanDays",
	Long:  `Purges entities older than purgeEntitiesOlderThanDays`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("Starting purge")

		accountName := viper.GetString("account-name")
		accountKey := viper.GetString("account-key")

		purger, err := purger.NewTablePurger(accountName, accountKey, tableName, purgeEntitiesOlderThanDays, periodLengthInHours, numWorkers, usePool, dryRun)
		if err != nil {
			log.Fatal(err)
		}

		period, err := util.ParsePeriod(startDate, endDate)
		if err != nil {
			result, err := purger.PurgeEntities()
			if err != nil {
				log.Fatal(err)
			}
			if result.HasErrors() {
				os.Exit(1)
			}
		} else {
			result, err := purger.PurgeEntitiesWithin(period)
			if err != nil {
				log.Fatal(err)
			}
			if result.HasErrors() {
				os.Exit(1)
			}
		}

	},
}

func init() {
	tableCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().IntVar(&purgeEntitiesOlderThanDays, "num-days-to-keep", 365, "Number of days to keep")
	purgeCmd.Flags().IntVar(&periodLengthInHours, "num-hours-per-worker", 24, "Number of hours per worker")

	purgeCmd.Flags().StringVar(&startDate, "start-date", "", "The start date")
	purgeCmd.Flags().StringVar(&endDate, "end-date", "", "The end date")

	purgeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
	purgeCmd.Flags().BoolVar(&usePool, "use-pool", false, "Enable worker pool mode")
}
