package main

import (
	"flag"
	"log"
	"os"

	"github.com/fabito/azure-storage-purger/pkg/test"
)

func main() {

	storageAccountNamePtr := flag.String("storageAccount", "", "storageAccount. (Required)")
	storageAccountKeyPtr := flag.String("storageAccountKey", "", "storageAccountKey. (Required)")
	tableNamePtr := flag.String("tableName", "", "TableName. (Required)")
	flag.Parse()

	if *storageAccountNamePtr == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	err := test.PopulateTable(*storageAccountNamePtr, *storageAccountKeyPtr, *tableNamePtr)

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

}
