package cmd

import (
	"github.com/fabito/azure-storage-purger/pkg/container"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ()

// statsCmd represents the populate command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Gather statistics about containers in a Storage Account",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		accountName := viper.GetString("account-name")
		accountKey := viper.GetString("account-key")

		// cmd.Flags().GetString()

		log.Debug(accountName)
		log.Debug(accountKey)

		s, err := container.NewContainerStatsGatherer(accountName, accountKey)
		if err != nil {
			log.Fatal(err)
		}

		containers, _ := s.GetContainerSizes()
		for _, c := range containers {
			log.Infof("%s : %d", c.Name, c.Size)
		}

	},
}

func init() {
	containerCmd.AddCommand(statsCmd)
	// statsCmd.Flags().IntVar(&maxNumberOfEntitiesPerPartition, "max-num-entities", 5000, "Number of entities per partition")
	// statsCmd.Flags().IntVar(&startYear, "start-year", 2018, "Star year for data generation")
}
