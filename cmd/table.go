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

	// os.Getenv("AZP_STORAGE_ACCOUNT_NAME")
	// os.Getenv("AZP_STORAGE_ACCOUNT_KEY")

	tableCmd.PersistentFlags().StringVar(&accountName, "account-name", "", "The storage account name")
	tableCmd.MarkPersistentFlagRequired("account-name")

	tableCmd.PersistentFlags().StringVar(&accountKey, "account-key", "", "The storage account key")
	tableCmd.MarkPersistentFlagRequired("account-key")

	tableCmd.PersistentFlags().StringVar(&tableName, "table-name", "", "The storage table name")
	tableCmd.MarkPersistentFlagRequired("table-name")
}
