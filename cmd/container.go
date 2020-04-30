package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ()

// containerCmd represents the table command
var containerCmd = &cobra.Command{
	Use:   "container",
	Short: "Commands for Azure Storage Container purge",
	Long:  `Commands for Azure Storage Container purge`,
}

func init() {
	rootCmd.AddCommand(containerCmd)

	containerCmd.PersistentFlags().String("account-name", "", "The storage account name")
	viper.BindPFlag("account-name", containerCmd.PersistentFlags().Lookup("account-name"))

	containerCmd.PersistentFlags().String("account-key", "", "The storage account key")
	viper.BindPFlag("account-key", containerCmd.PersistentFlags().Lookup("account-key"))

	// containerCmd.PersistentFlags().StringVar(&tableName, "table-name", "", "The storage table name")
	// containerCmd.MarkPersistentFlagRequired("table-name")

	// containerCmd.PersistentFlags().IntVar(&numWorkers, "num-workers", runtime.NumCPU()*4, "Number of workers. Default is cpus * 4")

}
