package cmd

import (
	"runtime"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	tableName  string
	numWorkers int
)

// tableCmd represents the table command
var tableCmd = &cobra.Command{
	Use:   "table",
	Short: "Commands for Azure Storage Table purge",
	Long:  `Commands for Azure Storage Table purge`,
}

func init() {
	rootCmd.AddCommand(tableCmd)

	tableCmd.PersistentFlags().String("account-name", "", "The storage account name")
	viper.BindPFlag("account-name", tableCmd.PersistentFlags().Lookup("account-name"))

	tableCmd.PersistentFlags().String("account-key", "", "The storage account key")
	viper.BindPFlag("account-key", tableCmd.PersistentFlags().Lookup("account-key"))

	tableCmd.PersistentFlags().StringVar(&tableName, "table-name", "", "The storage table name")
	tableCmd.MarkPersistentFlagRequired("table-name")

	tableCmd.PersistentFlags().IntVar(&numWorkers, "num-workers", runtime.NumCPU()*4, "Number of workers. Default is cpus * 4")

}
