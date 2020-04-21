package cmd

import (
	"github.com/fabito/azure-storage-purger/pkg/purger"
	log "github.com/sirupsen/logrus"
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
		log.Info("Starting purge")

		purger, err := purger.NewTablePurger(accountName, accountKey, tableName, purgeEntitiesOlderThanDays, dryRun)
		if err != nil {
			log.Fatal(err)
		}

		result, err := purger.PurgeEntities()
		if err != nil {
			log.Fatal(err)
		}
		log.Infof("%#v", result)
	},
}

func init() {
	tableCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().IntVar(&purgeEntitiesOlderThanDays, "num-days", 365, "Number of days to keep")
	purgeCmd.MarkFlagRequired("num-days")
	purgeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
}
