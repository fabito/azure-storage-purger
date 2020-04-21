package purger

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// PurgeResult details and metrics about the purge operation
type PurgeResult struct {
	PageCount      int64     `json:"page_count"`
	PartitionCount int64     `json:"partition_count"`
	RowCount       int64     `json:"row_count"`
	BatchCount     int64     `json:"batch_count"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	MinDate        time.Time `json:"min_date"`
	MaxDate        time.Time `json:"max_date"`
}

func (p *PurgeResult) addPageCount() {
	p.PageCount++
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
	if d.dryRun {
		log.Info("Dry run is enabled")
	}

	done := make(chan interface{})
	defer close(done)
	purgeResult := PurgeResult{StartTime: time.Now().UTC()}
	for queryResult := range d.queryResults(done, 120) {
		purgeResult.addPageCount()
		go func(r QueryResult) {
			if r.Error != nil {
				log.Warn("Error while querying table", r.Error)
			} else {
				d.processEntities(done, r)
			}
		}(queryResult)
	}
	purgeResult.EndTime = time.Now().UTC()
	return purgeResult, nil
}

func (d *DefaultTablePurger) queryResults(done <-chan interface{}, timeout uint) <-chan QueryResult {
	queryResultStream := make(chan QueryResult)
	partitionKey := GetMaximumPartitionKeyToDelete(d.purgeEntitiesOlderThanDays)

	queryOptions := &storage.QueryOptions{}
	queryOptions.Filter = fmt.Sprintf("PartitionKey le '%s'", partitionKey)
	queryOptions.Select = []string{"PartitionKey", "RowKey"}
	tableOptions := &storage.TableOptions{}
	log.Infof("Querying all records older than %d days", d.purgeEntitiesOlderThanDays)
	log.Info("Query options", queryOptions)
	go func() {
		defer close(queryResultStream)
		log.Debug("Fetching page")
		result, err := d.table.QueryEntities(timeout, storage.NoMetadata, queryOptions)
		queryResult := QueryResult{Error: err, EntityQueryResult: result}
		select {
		case <-done:
			return
		case queryResultStream <- queryResult:
		}

		for result.QueryNextLink.NextLink != nil {
			log.Debug("Fetching next page")
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

func (d *DefaultTablePurger) processEntities(done <-chan interface{}, queryResult QueryResult) {
	for batch := range d.batches(done, d.partitions(done, queryResult)) {
		go func(batch *storage.TableBatch) {
			log.Debugf("Executing batch with size %d", len(batch.BatchEntitySlice))
			if !d.dryRun {
				batch.ExecuteBatch()
			}
		}(batch)
	}
}

func (d *DefaultTablePurger) partitions(done <-chan interface{}, result QueryResult) <-chan Partition {
	yield := make(chan Partition)
	go func() {
		defer close(yield)
		// group entities by PartitionKey
		m := make(map[string][]*storage.Entity)
		for _, entity := range result.EntityQueryResult.Entities {
			m[entity.PartitionKey] = append(m[entity.PartitionKey], entity)
		}
		log.Debugf("Partioning result: %d", len(m))
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
