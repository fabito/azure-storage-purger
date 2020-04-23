package purger

import (
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// PurgeResult details and metrics about the purge operation
type PurgeResult struct {
	PageCount       int64     `json:"page_count"`
	PartitionCount  int64     `json:"partition_count"`
	RowCount        int64     `json:"row_count"`
	BatchCount      int64     `json:"batch_count"`
	BatchErrorCount int64     `json:"batch_error_count"`
	RowErrorCount   int64     `json:"row_error_count"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	// MinDate         time.Time `json:"min_date"`
	// MaxDate         time.Time `json:"max_date"`
}

func (p *PurgeResult) addPageCount() {
	p.PageCount++
}

func (p *PurgeResult) end() {
	p.EndTime = time.Now().UTC()
}

func (p *PurgeResult) computeTableBatchResult(result *TableBatchResult) {
	p.BatchCount++
	p.RowCount += result.batchSize()
	if result.Error != nil {
		p.BatchErrorCount++
		p.RowErrorCount += result.batchSize()
	}
}

// HasErrors whether or not any error occurred during the purge job
func (p *PurgeResult) HasErrors() bool {
	return p.BatchErrorCount > 0
}

// AzureTablePurger purges entities from Storage Tables
type AzureTablePurger interface {
	PurgeEntities() (PurgeResult, error)
}

// DefaultTablePurger default table purger
type DefaultTablePurger struct {
	tableName                  string
	purgeEntitiesOlderThanDays int
	periodLengthInDays         int
	numWorkers                 int
	table                      *storage.Table
	dryRun                     bool
	result                     PurgeResult
}

// NewTablePurgerWithClient creates a new Basic Purger
func NewTablePurgerWithClient(client storage.Client, accountName, accountKey, tableName string, purgeEntitiesOlderThanDays, periodLengthInDays, numWorkers int, dryRun bool) (AzureTablePurger, error) {
	purger := &DefaultTablePurger{
		tableName:                  tableName,
		purgeEntitiesOlderThanDays: purgeEntitiesOlderThanDays,
		periodLengthInDays:         periodLengthInDays,
		numWorkers:                 numWorkers,
		dryRun:                     dryRun,
	}
	tableService := client.GetTableService()
	table := tableService.GetTableReference(purger.tableName)
	purger.table = table
	return purger, nil
}

// NewTablePurger creates a new Basic Purger
func NewTablePurger(accountName, accountKey, tableName string, purgeEntitiesOlderThanDays, periodLengthInDays, numWorkers int, dryRun bool) (AzureTablePurger, error) {
	client, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	return NewTablePurgerWithClient(client, accountName, accountKey, tableName, purgeEntitiesOlderThanDays, periodLengthInDays, numWorkers, dryRun)
}

// QueryResult groups 2 query possible outcomes
type QueryResult struct {
	Error             error
	EntityQueryResult *storage.EntityQueryResult
}

// TableBatchResult holds the result of a batch operation
type TableBatchResult struct {
	Error error
	Batch *storage.TableBatch
}

func (t *TableBatchResult) batchSize() int64 {
	return int64(len(t.Batch.BatchEntitySlice))
}

// Partition contains the entities grouped by partition
type Partition struct {
	key      string
	entities []*storage.Entity
}

// PurgeEntities sdf
func (d *DefaultTablePurger) PurgeEntities() (PurgeResult, error) {
	if d.dryRun {
		log.Warn("Dry run is ENABLED")
	}
	d.result = PurgeResult{StartTime: time.Now().UTC()}
	done := make(chan interface{})
	defer close(done)
	var timeout uint = 120

	startPartitionKey, err := d.getOldestPartition(timeout)

	if err != nil {
		d.result.end()
		return d.result, err
	}

	endPartitionKey := GetMaximumPartitionKeyToDelete(d.purgeEntitiesOlderThanDays)
	start := timeFromTicksAscendingWithLeadingZero(startPartitionKey)
	end := timeFromTicksAscendingWithLeadingZero(endPartitionKey)

	if start == end || start.After(end) {
		log.Warnf("Start date (%s) should be greater than end date (%s)", start, end)
		d.result.end()
		return d.result, err
	}

	log.Infof("Starting purging all entities created between %s and %s", start, end)

	process := func(batches <-chan *storage.TableBatch) <-chan *TableBatchResult {
		processedBatchStream := make(chan *TableBatchResult)
		go func() {
			defer close(processedBatchStream)
			for batch := range batches {
				log.Debugf("Executing table batch with size %d", len(batch.BatchEntitySlice))
				result := &TableBatchResult{Batch: batch}
				if !d.dryRun {
					err := batch.ExecuteBatch()
					result.Error = err
					if err != nil {
						log.Error(err)
					}
				}
				select {
				case <-done:
					return
				case processedBatchStream <- result:
				}
			}
		}()
		return processedBatchStream
	}

	numProcessors := d.numWorkers
	log.Infof("Spinning up %d batch processors.\n", numProcessors)
	period := Period{Start: start, End: end}
	splits := period.SplitsFrom(numProcessors)
	logPeriods(splits)
	processors := make([]<-chan *TableBatchResult, len(splits))
	for i := 0; i < len(splits); i++ {
		split := splits[i]
		processor := process(d.batches(done, d.partitions(done, d.queryResultsGenerator(done, d.periodQueryOptionsGenerator2(done, split.Start, split.End, d.periodLengthInDays), timeout))))
		processors[i] = processor
	}

	for processedBatch := range FanIn(done, processors...) {
		d.result.computeTableBatchResult(processedBatch)
	}

	d.result.end()
	return d.result, nil
}

func (d *DefaultTablePurger) queryResultsGenerator(done <-chan interface{}, queryOptionsStream <-chan *storage.QueryOptions, timeout uint) <-chan QueryResult {
	queryResultStream := make(chan QueryResult)
	go func() {
		defer close(queryResultStream)
		for queryOptions := range queryOptionsStream {
			log.Debug("Querying entities using: ", queryOptions)
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
			for result != nil && result.QueryNextLink.NextLink != nil {
				pageCount++
				log.Debugf("Fetching next page %d", pageCount)
				result, err = result.NextResults(tableOptions)
				if err != nil {
					log.Warnf("Error fetching page %d", pageCount)
					log.Error(err)
				}
				if pageCount%100 == 0 {
					log.Infof("Processed %d pages.", pageCount)
				}
				queryResult := QueryResult{Error: err, EntityQueryResult: result}
				select {
				case <-done:
					return
				case queryResultStream <- queryResult:
				}
			}
			log.Infof("Processed %d pages. QueryOptions %#v", pageCount, queryOptions)
		}

	}()
	return queryResultStream
}

func (d *DefaultTablePurger) partitions(done <-chan interface{}, queryResults <-chan QueryResult) <-chan Partition {
	yield := make(chan Partition)
	go func() {
		defer close(yield)
		for result := range queryResults {

			if result.Error != nil {
				// TODO Compute metric errors
				log.Warn("Skipping query result in failed state.")
				continue
			}

			m := make(map[string][]*storage.Entity)

			for _, entity := range result.EntityQueryResult.Entities {
				m[entity.PartitionKey] = append(m[entity.PartitionKey], entity)
			}
			log.Debugf("Partioning query result: %d", len(m))
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
			entities := p.entities
			count := len(entities)
			log.Tracef("Chunkfying partition (%s) with %d entities", p.key, count)
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
		log.Infof("Oldest partition key in '%s' table is %s (%s)", d.tableName, oldestPartitionKey, timeFromTicksAscendingWithLeadingZero(oldestPartitionKey))
		return oldestPartitionKey, nil
	}

	return "", errors.New("Oldest record not found")
}

func (d *DefaultTablePurger) periodQueryOptionsGenerator(done <-chan interface{}, start, end time.Time, periodLengthInDays int) <-chan *storage.QueryOptions {
	queryOptionsStream := make(chan *storage.QueryOptions)
	go func() {
		defer close(queryOptionsStream)
		for d := start; d.After(end) == false; d = d.AddDate(0, 0, periodLengthInDays) {
			from := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
			to := from.AddDate(0, 0, periodLengthInDays-1)
			if to.After(end) {
				to = time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC)
			} else {
				to = time.Date(to.Year(), to.Month(), to.Day(), 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC)
			}

			log.Infof("Creating queryOptions: from %s to %s", from, to)
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

func (d *DefaultTablePurger) periodQueryOptionsGenerator2(done <-chan interface{}, start, end time.Time, periodLengthInDays int) <-chan *storage.QueryOptions {
	queryOptionsStream := make(chan *storage.QueryOptions)
	go func() {
		defer close(queryOptionsStream)
		from := start
		to := end
		log.Infof("Creating queryOptions: from %s to %s", from, to)
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
	}()
	return queryOptionsStream
}
