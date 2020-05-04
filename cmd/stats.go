package cmd

import (
	"github.com/fabito/azure-storage-purger/pkg/container"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ()

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "(Experimental) Gather statistics about all containers in a Storage Account",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		accountName := viper.GetString("account-name")
		accountKey := viper.GetString("account-key")

		s, err := container.NewStatsGatherer(accountName, accountKey)
		if err != nil {
			log.Fatal(err)
		}

		containers, _ := s.GatherStatistics()
		for _, c := range containers {
			log.Info(c)
		}

	},
}

func init() {
	containerCmd.AddCommand(statsCmd)
}
