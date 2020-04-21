package purger

import (
	"errors"
	"fmt"
	"sync"
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

func (p *PurgeResult) end() {
	p.EndTime = time.Now().UTC()
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
	var wg sync.WaitGroup
	var timeout uint = 120
	purgeResult := PurgeResult{StartTime: time.Now().UTC()}

	startPartitionKey, err := d.getOldestPartition(timeout)

	if err != nil {
		purgeResult.end()
		return purgeResult, err
	}

	endPartitionKey := GetMaximumPartitionKeyToDelete(d.purgeEntitiesOlderThanDays)
	start := timeFromTicksAscendingWithLeadingZero(startPartitionKey)
	end := timeFromTicksAscendingWithLeadingZero(endPartitionKey)

	if start.After(end) {
		log.Warnf("Start date (%s) is greater than end date (%s)", start, end)
		purgeResult.end()
		return purgeResult, err
	}

	log.Debugf("Starting purging all entities created between %s and %s", start, end)

	for batch := range d.batches(done, d.partitions2(done, d.queryResultsGenerator(done, d.dailyQueryOptionsGenerator(done, start, end), timeout))) {
		wg.Add(1)
		go func(batch *storage.TableBatch) {
			log.Debugf("Executing batch with size %d", len(batch.BatchEntitySlice))
			if !d.dryRun {
				err := batch.ExecuteBatch()
				if err != nil {
					log.Errorf("Error executing batch: %#v", err)
				}
			}
			wg.Done()
		}(batch)
	}
	wg.Wait()
	purgeResult.end()
	return purgeResult, nil
}

// PurgeEntities2 sdf
func (d *DefaultTablePurger) PurgeEntities2() (PurgeResult, error) {
	if d.dryRun {
		log.Info("Dry run is enabled")
	}

	done := make(chan interface{})
	defer close(done)
	var wg sync.WaitGroup
	purgeResult := PurgeResult{StartTime: time.Now().UTC()}

	for batch := range d.batches(done, d.partitions2(done, d.queryResults(done, 120))) {
		wg.Add(1)
		go func(batch *storage.TableBatch) {
			log.Debugf("Executing batch with size %d", len(batch.BatchEntitySlice))
			if !d.dryRun {
				err := batch.ExecuteBatch()
				if err != nil {
					log.Errorf("Error executing batch: %#v", err)
				}
			}
			wg.Done()
		}(batch)
	}
	wg.Wait()
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

func (d *DefaultTablePurger) queryResultsGenerator(done <-chan interface{}, queryOptionsStream <-chan *storage.QueryOptions, timeout uint) <-chan QueryResult {
	queryResultStream := make(chan QueryResult)
	go func() {
		defer close(queryResultStream)
		for queryOptions := range queryOptionsStream {
			log.Info("Query options: ", queryOptions)
			pageCount := 1
			log.Debugf("Fetching page %d", pageCount)
			result, err := d.table.QueryEntities(timeout, storage.NoMetadata, queryOptions)
			queryResult := QueryResult{Error: err, EntityQueryResult: result}
			select {
			case <-done:
				return
			case queryResultStream <- queryResult:
			}
			tableOptions := &storage.TableOptions{}
			for result.QueryNextLink.NextLink != nil {
				pageCount++
				log.Debugf("Fetching next page %d", pageCount)
				result, err = result.NextResults(tableOptions)
				queryResult := QueryResult{Error: err, EntityQueryResult: result}
				select {
				case <-done:
					return
				case queryResultStream <- queryResult:
				}
			}
		}
	}()
	return queryResultStream
}

func (d *DefaultTablePurger) partitions2(done <-chan interface{}, queryResults <-chan QueryResult) <-chan Partition {
	yield := make(chan Partition)
	go func() {
		defer close(yield)
		for result := range queryResults {
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
			log.Debugf("Chunkfying %s with %d entitie(s)", p.key, len(p.entities))
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

// works for tables where the PartitionKey has fized-length zero padded strings
func (d *DefaultTablePurger) getOldestPartition(timeout uint) (string, error) {
	queryOptions := &storage.QueryOptions{}
	queryOptions.Filter = fmt.Sprintf("PartitionKey ne '%s'", "")
	queryOptions.Select = []string{"PartitionKey"}
	queryOptions.Top = 1
	log.Debugf("Fetching oldest partition key for table %s with query %#v", d.tableName, queryOptions)
	result, err := d.table.QueryEntities(timeout, storage.NoMetadata, queryOptions)
	if err != nil {
		log.Error("Error fetching oldest partition key", err)
		return "", err
	}

	if len(result.Entities) > 0 {
		oldestEntity := result.Entities[0]
		oldestPartitionKey := oldestEntity.PartitionKey
		log.Debugf("Oldest partition key is %s (%s)", oldestPartitionKey, timeFromTicksAscendingWithLeadingZero(oldestPartitionKey))
		return oldestPartitionKey, nil
	}

	return "", errors.New("Oldest record not found")
}

func (d *DefaultTablePurger) dailyQueryOptionsGenerator(done <-chan interface{}, start, end time.Time) <-chan *storage.QueryOptions {
	queryOptionsStream := make(chan *storage.QueryOptions)
	go func() {
		defer close(queryOptionsStream)
		for d := start; d.After(end) == false; d = d.AddDate(0, 0, 1) {
			from := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
			to := time.Date(d.Year(), d.Month(), d.Day(), 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC)
			log.Debugf("Generating queries from %s to %s", from, to)
			fromTicks := TicksAscendingWithLeadingZero(TicksFromTime(from))
			toTicks := TicksAscendingWithLeadingZero(TicksFromTime(to))
			queryOptions := &storage.QueryOptions{}
			queryOptions.Filter = fmt.Sprintf("PartitionKey ge '%s' and PartitionKey lt '%s'", fromTicks, toTicks)
			queryOptions.Select = []string{"PartitionKey", "RowKey"}
			select {
			case <-done:
				return
			case queryOptionsStream <- queryOptions:
			}
		}
	}()
	return queryOptionsStream
}
