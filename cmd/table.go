package cmd

import (
	"github.com/spf13/cobra"
)

var (
	accountName string
	accountKey  string
	tableName   string
)

// tableCmd represents the table command
var tableCmd = &cobra.Command{
	Use:   "table",
	Short: "Commands for Azure Storage Table purge",
	Long:  `Commands for Azure Storage Table purge`,
}

func init() {
	rootCmd.AddCommand(tableCmd)
	tableCmd.PersistentFlags().StringVar(&accountName, "accountName", "", "The storage account name")
	tableCmd.MarkFlagRequired("accountName")

	tableCmd.PersistentFlags().StringVar(&accountKey, "accountKey", "", "The storage account key")
	tableCmd.MarkFlagRequired("accountKey")

	tableCmd.PersistentFlags().StringVar(&tableName, "tableName", "", "The storage table name")
	tableCmd.MarkFlagRequired("tableName")
}
