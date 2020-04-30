# `azp` 

Command line utility for purging data from Azure Storage Tables

**Important:** Only works for tables using "ticks ascending with leading zero padded with zeroes" as `PartitionKey`

Deleting lots of entities is very time consuming - we have to fetch first in order to delete them. Ideally we should partition the data into multiple tables (i.e. daily, monthly) then there is no need to fetch entities and entire tables can be deleted.

## Usage

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

## Examples

### Purging entities

``` bash
azp table purge \
    --account-name $STORAGE_ACCOUNT_NAME  \
    --account-key $STORAGE_ACCOUNT_KEY \
    --table-name "logs" \
    --num-workers 64 \
    --num-days-to-keep 30  \
    -v info
```

### Create and populate a testing table

```bash
azp table populate \
    --account-name $STORAGE_ACCOUNT_NAME  \
    --account-key $STORAGE_ACCOUNT_KEY \
    --table-name "logs" \
    -v info \
    --max-num-entities 1 \
    --start-year 2019
```