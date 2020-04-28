package cmd

import (
	"io"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var v string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "azp",
	Short: "Utility for purging data from Azure Storage Tables",
	Long:  ``,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logrus.Println(err)
		os.Exit(1)
	}
}

func setUpLogs(out io.Writer, level string) error {
	logrus.SetOutput(out)
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logrus.SetLevel(lvl)
	return nil
}

func init() {
	cobra.OnInitialize()
	viper.SetEnvPrefix("azp") // Set the environment prefix to AZP_*
	viper.AutomaticEnv()      // Automatically search for environment variables
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if err := setUpLogs(os.Stdout, v); err != nil {
			return err
		}
		return nil
	}
	rootCmd.PersistentFlags().StringVarP(&v, "verbosity", "v", logrus.InfoLevel.String(), "Log level (debug, info, warn, error, fatal, panic")
}
