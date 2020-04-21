package cmd

import (
	"os"

	"github.com/fabito/azure-storage-purger/pkg/purger"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	purgeEntitiesOlderThanDays int
	dryRun                     bool
)

// purgeCmd represents the purge command
var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purges entities older than purgeEntitiesOlderThanDays",
	Long:  `Purges entities older than purgeEntitiesOlderThanDays`,
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Info("Starting purge")

		purger, err := purger.NewTablePurger(accountName, accountKey, tableName, purgeEntitiesOlderThanDays, dryRun)
		if err != nil {
			logrus.Fatal(err)
			os.Exit(1)
		}

		result, err := purger.PurgeEntities()
		if err != nil {
			logrus.Fatal(err)
			os.Exit(1)
		}
		logrus.Info("Result", result)
	},
}

func init() {
	tableCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().IntVar(&purgeEntitiesOlderThanDays, "num-days", 365, "Number of days to keep")
	purgeCmd.MarkFlagRequired("num-days")
	purgeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
}
