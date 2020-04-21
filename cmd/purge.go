package cmd

import (
	"log"
	"os"

	"github.com/fabito/azure-storage-purger/pkg/purger"
	"github.com/spf13/cobra"
)

var (
	purgeEntitiesOlderThanDays int
)

// purgeCmd represents the purge command
var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purges entities older than purgeEntitiesOlderThanDays",
	Long:  `Purges entities older than purgeEntitiesOlderThanDays`,
	Run: func(cmd *cobra.Command, args []string) {
		purger, err := purger.NewTablePurger(accountName, accountKey, tableName, purgeEntitiesOlderThanDays)

		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		err = purger.PurgeEntities()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

	},
}

func init() {
	tableCmd.AddCommand(purgeCmd)
	populateCmd.LocalNonPersistentFlags().IntVar(&purgeEntitiesOlderThanDays, "purgeEntitiesOlderThanDays", 30, "A help for foo")
	populateCmd.MarkFlagRequired("purgeEntitiesOlderThanDays")
}
