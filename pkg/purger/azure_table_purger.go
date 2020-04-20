package purger

import (
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// AzureTablePurger purges entities from Storage Tables
type AzureTablePurger interface {
	PurgeEntities()
}

// DefaultTablePurger default table purger
type DefaultTablePurger struct {
	storageAccountName         string
	storageAccountKey          string
	tableName                  string
	purgeEntitiesOlderThanDays int
	table                      *storage.Table
}

// NewTablePurger creates a new Basic Purger
func NewTablePurger(accountName, accountKey, tableName string, purgeEntitiesOlderThanDays int) (*DefaultTablePurger, error) {
	purger := &DefaultTablePurger{
		storageAccountName:         accountName,
		storageAccountKey:          accountKey,
		tableName:                  tableName,
		purgeEntitiesOlderThanDays: purgeEntitiesOlderThanDays,
	}
	client, err := storage.NewBasicClient(purger.storageAccountName, purger.storageAccountKey)

	if err != nil {
		return nil, err
	}

	tableService := client.GetTableService()
	table := tableService.GetTableReference(purger.tableName)
	purger.table = table

	return purger, nil
}

// PurgeEntities sdf
func (d *DefaultTablePurger) PurgeEntities() {

	partitionKey := GetMaximumPartitionKeyToDelete(d.purgeEntitiesOlderThanDays)
	queryOptions := &storage.QueryOptions{}
	queryOptions.Filter = fmt.Sprintf("PartitionKey le '%s'", partitionKey)
	queryOptions.Select = []string{"PartitionKey", "RowKey"}

	// var rowCount int = 0
	result, err := d.table.QueryEntities(120, storage.FullMetadata, queryOptions)
	if err != nil {
		log.Fatal(err)
		return
	}

	d.processEntities(result)

	tableOptions := &storage.TableOptions{}
	for result.QueryNextLink.NextLink != nil {
		result, _ = result.NextResults(tableOptions)
		d.processEntities(result)
	}

}

func (d *DefaultTablePurger) processEntities(queryResult *storage.EntityQueryResult) {
	rowCount := len(queryResult.Entities)
	fmt.Println(rowCount)

	// group entities by PartitionKey
	var m map[string][]*storage.Entity
	m = make(map[string][]*storage.Entity)
	for _, entity := range queryResult.Entities {
		m[entity.PartitionKey] = append(m[entity.PartitionKey], entity)
	}

	// var batches []storage.TableBatch
	chunkSize := 100
	for partitionKey, entities := range m {
		count := len(entities)
		for i := 0; i < count; i += chunkSize {
			end := i + chunkSize
			if end > count {
				end = count
			}
			d.purge(partitionKey, entities[i:end])
		}
	}
}

func (d *DefaultTablePurger) purge(partitionKey string, entities []*storage.Entity) {
	tableBatch := d.table.NewBatch()
	for _, entity := range entities {
		tableBatch.DeleteEntityByForce(entity, true)
	}

	fmt.Printf(" %s : %d\n", timeFromTicksAscendingWithLeadingZero(tableBatch.BatchEntitySlice[0].PartitionKey), len(tableBatch.BatchEntitySlice))

	tableBatch.ExecuteBatch()
}
