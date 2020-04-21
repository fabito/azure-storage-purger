package purger

import (
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// PurgeResult details and metrics about the purge operation
type PurgeResult struct {
	PageCount      int64
	PartitionCount int64
	RowCount       int64
	BatchCount     int64
	StartTime      time.Time
	EndTime        time.Time
	MinDate        time.Time
	MaxDate        time.Time
}

// AzureTablePurger purges entities from Storage Tables
type AzureTablePurger interface {
	PurgeEntities() (PurgeResult, error)
}

// DefaultTablePurger default table purger
type DefaultTablePurger struct {
	storageAccountName         string
	storageAccountKey          string
	tableName                  string
	purgeEntitiesOlderThanDays int
	table                      *storage.Table
	dryRun                     bool
}

// NewTablePurger creates a new Basic Purger
func NewTablePurger(accountName, accountKey, tableName string, purgeEntitiesOlderThanDays int, dryRun bool) (*DefaultTablePurger, error) {
	purger := &DefaultTablePurger{
		storageAccountName:         accountName,
		storageAccountKey:          accountKey,
		tableName:                  tableName,
		purgeEntitiesOlderThanDays: purgeEntitiesOlderThanDays,
		dryRun:                     dryRun,
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

// QueryResult groups 2 query possible outcomes
type QueryResult struct {
	Error             error
	EntityQueryResult *storage.EntityQueryResult
}

// Partition contains the entities grouped by partition
type Partition struct {
	key      string
	entities []*storage.Entity
}

// PurgeEntities sdf
func (d *DefaultTablePurger) PurgeEntities() (PurgeResult, error) {
	done := make(chan interface{})
	defer close(done)
	purgeResult := PurgeResult{}
	for queryResult := range d.queryResults(done, 120) {
		go func(r QueryResult) {
			if r.Error != nil {

			} else {
				d.processEntities(done, r)
			}
		}(queryResult)
	}
	return purgeResult, nil
}

func (d *DefaultTablePurger) processEntities(done <-chan interface{}, queryResult QueryResult) {
	for batch := range d.batches(done, d.partitions(done, queryResult)) {
		go func(batch *storage.TableBatch) {
			if !d.dryRun {
				batch.ExecuteBatch()
			}
		}(batch)
	}
}

func (d *DefaultTablePurger) queryResults(done <-chan interface{}, timeout uint) <-chan QueryResult {
	queryResultStream := make(chan QueryResult)
	partitionKey := GetMaximumPartitionKeyToDelete(d.purgeEntitiesOlderThanDays)
	queryOptions := &storage.QueryOptions{}
	queryOptions.Filter = fmt.Sprintf("PartitionKey le '%s'", partitionKey)
	queryOptions.Select = []string{"PartitionKey", "RowKey"}
	tableOptions := &storage.TableOptions{}
	go func() {
		defer close(queryResultStream)
		result, err := d.table.QueryEntities(timeout, storage.NoMetadata, queryOptions)
		queryResult := QueryResult{Error: err, EntityQueryResult: result}
		select {
		case <-done:
			return
		case queryResultStream <- queryResult:
		}

		for result.QueryNextLink.NextLink != nil {
			result, err = result.NextResults(tableOptions)
			queryResult := QueryResult{Error: err, EntityQueryResult: result}
			select {
			case <-done:
				return
			case queryResultStream <- queryResult:
			}
		}
	}()
	return queryResultStream
}

func (d *DefaultTablePurger) partitions(done <-chan interface{}, result QueryResult) <-chan Partition {
	yield := make(chan Partition)
	go func() {
		defer close(yield)
		// group entities by PartitionKey
		var m map[string][]*storage.Entity
		m = make(map[string][]*storage.Entity)
		for _, entity := range result.EntityQueryResult.Entities {
			m[entity.PartitionKey] = append(m[entity.PartitionKey], entity)
		}
		for k, v := range m {
			partition := Partition{key: k, entities: v}
			select {
			case <-done:
				return
			case yield <- partition:
			}
		}

	}()
	return yield
}

func (d *DefaultTablePurger) batches(done <-chan interface{}, partitions <-chan Partition) <-chan *storage.TableBatch {
	yield := make(chan *storage.TableBatch)
	chunkSize := 100
	go func() {
		defer close(yield)
		for p := range partitions {
			entities := p.entities
			count := len(entities)
			for i := 0; i < count; i += chunkSize {
				end := i + chunkSize
				if end > count {
					end = count
				}
				tableBatch := d.table.NewBatch()
				for _, entity := range entities[i:end] {
					tableBatch.DeleteEntityByForce(entity, true)
				}
				select {
				case <-done:
					return
				case yield <- tableBatch:
				}
			}
		}
	}()
	return yield
}
