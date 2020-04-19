package main

import (
	"flag"
	"log"
	"os"

	"github.com/fabito/azure-storage-purger/pkg/purger"
)

func main() {

	storageAccountNamePtr := flag.String("storageAccount", "", "storageAccount. (Required)")
	storageAccountKeyPtr := flag.String("storageAccountKey", "", "storageAccountKey. (Required)")
	tableNamePtr := flag.String("tableName", "", "TableName. (Required)")
	purgeEntitiesOlderThanDaysPtr := flag.Int("purgeEntitiesOlderThanDays", 30, "purgeEntitiesOlderThanDays.")
	flag.Parse()

	if *storageAccountNamePtr == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	purger, err := purger.NewTablePurger(*storageAccountNamePtr, *storageAccountKeyPtr, *tableNamePtr, *purgeEntitiesOlderThanDaysPtr)

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	purger.PurgeEntities()

}
