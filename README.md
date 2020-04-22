# `azp` CLI utility to purge data from Azure Storage Tables

``` bash
$ bin/azp table --help
Commands for Azure Storage Table purge

Usage:
  azp table [command]

Available Commands:
  populate    Add dummy data to Azure Storage Table
  purge       Purges entities older than purgeEntitiesOlderThanDays

Flags:
      --account-key string    The storage account key
      --account-name string   The storage account name
  -h, --help                  help for table
      --table-name string     The storage table name

Global Flags:
  -v, --verbosity string   Log level (debug, info, warn, error, fatal, panic (default "info")

Use "azp table [command] --help" for more information about a command.
```