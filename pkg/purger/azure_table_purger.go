package purger

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fabito/azure-storage-purger/pkg/metrics"
	"github.com/fabito/azure-storage-purger/pkg/util"
	"github.com/fabito/azure-storage-purger/pkg/work"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	timeout = 30
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
}

func (p *PurgeResult) addPageCount() {
	p.PageCount++
}

func (p *PurgeResult) end(metrics *metrics.Metrics) {
	p.EndTime = time.Now().UTC()
	p.BatchCount = metrics.BatchCount()
	p.BatchErrorCount = metrics.BatchErrorCount()
	p.RowCount = metrics.EntityCount()
}

func (p *PurgeResult) computeTableBatchResult(result *TableBatchResult) {
	// p.BatchCount++
	// p.RowCount += result.batchSize()
	// if result.Error != nil {
	// 	p.BatchErrorCount++
	// 	p.RowErrorCount += result.batchSize()
	// }
}

// HasErrors whether or not any error occurred during the purge job
func (p *PurgeResult) HasErrors() bool {
	return p.BatchErrorCount > 0
}

// AzureTablePurger purges entities from Storage Tables
type AzureTablePurger interface {
	PurgeEntities() (PurgeResult, error)
	PurgeEntitiesWithin(period *util.Period) (PurgeResult, error)
}

// DefaultTablePurger default table purger
type DefaultTablePurger struct {
	tableName                  string
	purgeEntitiesOlderThanDays int
	periodLengthInHours        int
	numWorkers                 int
	table                      *storage.Table
	usePool                    bool
	dryRun                     bool
	result                     PurgeResult
	Metrics                    *metrics.Metrics
}

// NewTablePurgerWithClient creates a new Basic Purger
func NewTablePurgerWithClient(client storage.Client, accountName, accountKey, tableName string, purgeEntitiesOlderThanDays, periodLengthInHours, numWorkers int, usePool, dryRun bool) (AzureTablePurger, error) {
	purger := &DefaultTablePurger{
		tableName:                  tableName,
		purgeEntitiesOlderThanDays: purgeEntitiesOlderThanDays,
		periodLengthInHours:        periodLengthInHours,
		numWorkers:                 numWorkers,
		dryRun:                     dryRun,
		usePool:                    usePool,
		Metrics:                    metrics.NewMetrics(),
	}
	if log.IsLevelEnabled(log.TraceLevel) {
		client.Sender = util.SenderWithLogging(client.Sender)
	}
	tableService := client.GetTableService()
	table := tableService.GetTableReference(purger.tableName)
	purger.table = table

	return purger, nil
}

// NewTablePurger creates a new Basic Purger
func NewTablePurger(accountName, accountKey, tableName string, purgeEntitiesOlderThanDays, periodLengthInHours, numWorkers int, usePool, dryRun bool) (AzureTablePurger, error) {

	// NewClientFromConnectionString
	client, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	return NewTablePurgerWithClient(client, accountName, accountKey, tableName, purgeEntitiesOlderThanDays, periodLengthInHours, numWorkers, usePool, dryRun)
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

type batchProcessor struct {
	input   <-chan *storage.TableBatch
	metrics *metrics.Metrics
	dryRun  bool
}

func (t *batchProcessor) Task() {
	for batch := range t.input {
		t.metrics.RegisterTableBatchAttempt()
		start := time.Now()
		if !t.dryRun {
			err := batch.ExecuteBatch()
			if err != nil {
				log.Error(err)
				t.metrics.RegisterTableBatchFailed()
			} else {
				t.metrics.RegisterEntitiesProcessed(int64(len(batch.BatchEntitySlice)))
				t.metrics.RegisterTableBatchDurationSince(start)
				t.metrics.RegisterTableBatchSuccess()
			}
		}
	}
}

func (d *DefaultTablePurger) purgeEntitiesUsingWorkerPool(done chan interface{}, period *util.Period) (PurgeResult, error) {

	p := work.New(d.numWorkers)
	var wg sync.WaitGroup
	periods := period.Split(time.Duration(d.periodLengthInHours) * time.Hour)
	for _, split := range periods {
		batchChannel := d.batches(done, d.partitions(done, d.queryResultsGenerator(done, d.periodQueryOptionsGenerator(done, split.Start, split.End), timeout)))
		wg.Add(1)
		// FIXME use done channel
		job := batchProcessor{metrics: d.Metrics, input: batchChannel, dryRun: d.dryRun}
		go func() {
			p.Run(&job)
			wg.Done()
		}()
	}
	wg.Wait()
	return d.result, nil
}

func (d *DefaultTablePurger) purgeEntitiesUsingFanIn(done chan interface{}, period *util.Period) (PurgeResult, error) {
	process := func(batches <-chan *storage.TableBatch) <-chan *TableBatchResult {
		processedBatchStream := make(chan *TableBatchResult)
		go func() {
			defer close(processedBatchStream)
			for batch := range batches {
				d.Metrics.RegisterTableBatchAttempt()
				log.Debugf("Executing table batch with size %d", len(batch.BatchEntitySlice))
				result := &TableBatchResult{Batch: batch}
				if !d.dryRun {
					start := time.Now()
					err := batch.ExecuteBatch()
					result.Error = err
					if err != nil {
						d.Metrics.RegisterTableBatchFailed()
						log.Error(err)
					} else {
						d.Metrics.RegisterTableBatchDurationSince(start)
						d.Metrics.RegisterEntitiesProcessed(int64(len(batch.BatchEntitySlice)))
						d.Metrics.RegisterTableBatchSuccess()
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
	splits := period.SplitsFrom(numProcessors)
	util.LogPeriods(splits)
	processors := make([]<-chan *TableBatchResult, len(splits))
	for i := 0; i < len(splits); i++ {
		split := splits[i]
		processor := process(d.batches(done, d.partitions(done, d.queryResultsGenerator(done, d.periodQueryOptionsGenerator(done, split.Start, split.End), timeout))))
		processors[i] = processor
	}

	for processedBatch := range FanIn(done, processors...) {
		d.result.computeTableBatchResult(processedBatch)
	}
	return d.result, nil
}

// PurgeEntities sdf
func (d *DefaultTablePurger) PurgeEntities() (PurgeResult, error) {
	startPartitionKey, err := d.getOldestPartition(timeout)
	if err != nil {
		d.result.end(d.Metrics)
		return d.result, err
	}
	endPartitionKey := util.GetMaximumPartitionKeyToDelete(d.purgeEntitiesOlderThanDays)
	start := util.TimeFromTicksAscendingWithLeadingZero(startPartitionKey)
	end := util.TimeFromTicksAscendingWithLeadingZero(endPartitionKey)

	if start == end || start.After(end) {
		log.Warnf("Start date (%s) should be greater than end date (%s)", start, end)
		d.result.end(d.Metrics)
		return d.result, err
	}
	period, _ := util.NewPeriod(start, end)
	return d.PurgeEntitiesWithin(period)
}

// PurgeEntitiesWithin all entities within Period
func (d *DefaultTablePurger) PurgeEntitiesWithin(period *util.Period) (PurgeResult, error) {
	if d.dryRun {
		log.Warn("Dry run is ENABLED")
	}
	d.result = PurgeResult{StartTime: time.Now().UTC()}
	done := make(chan interface{})
	defer close(done)

	log.Infof("Starting purging all entities created between %s and %s", period.Start, period.End)

	go d.Metrics.Log()

	if d.usePool {
		log.Info("Using worker pool implementation")
		d.purgeEntitiesUsingWorkerPool(done, period)
	} else {
		d.purgeEntitiesUsingFanIn(done, period)
	}

	log.Info("Summary")
	summaryLines := strings.Split(d.Metrics.String(), "\n")
	for _, line := range summaryLines {
		log.Info(line)
	}
	d.result.end(d.Metrics)

	log.Infof("It took %s", d.result.EndTime.Sub(d.result.StartTime))
	log.Infof("To delete %d entities in %d batches", d.result.RowCount, d.result.BatchCount)
	log.Infof("Errors in %d batches", d.result.BatchErrorCount)

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
			start := time.Now()
			d.Metrics.RegisterPageAttempt()
			result, err := d.table.QueryEntities(timeout, storage.NoMetadata, queryOptions)
			if err == nil {
				d.Metrics.RegisterPageDurationSince(start)
			} else {
				d.Metrics.RegisterPageFailed()
			}
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
				d.Metrics.RegisterPageAttempt()
				start = time.Now()
				result, err = result.NextResults(tableOptions)
				if err != nil {
					d.Metrics.RegisterPageFailed()
					log.Error(err)
				} else {
					d.Metrics.RegisterPageDurationSince(start)
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
			log.Debugf("Processed %d pages. QueryOptions %#v", pageCount, queryOptions)
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
				log.Error(result.Error)
				continue
			}

			m := make(map[string][]*storage.Entity)

			for _, entity := range result.EntityQueryResult.Entities {
				m[entity.PartitionKey] = append(m[entity.PartitionKey], entity)
			}
			log.Debugf("Partioning query result: %d", len(m))
			d.Metrics.RegisterPartitionsProcessed(int64(len(m)))
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
	queryOptions.Top = 100
	log.Debugf("Fetching oldest partition key for table %s with query %#v", d.tableName, queryOptions)
	result, err := d.table.QueryEntities(timeout, storage.NoMetadata, queryOptions)
	if err != nil {
		log.Error("Error fetching oldest partition key", err)
		return "", err
	}

	if len(result.Entities) <= 0 {
		tableOptions := &storage.TableOptions{}
		for result != nil && result.QueryNextLink.NextLink != nil {
			result, err = result.NextResults(tableOptions)
			if err != nil {
				log.Error("Error fetching oldest partition key", err)
				return "", err
			}
			if result != nil && len(result.Entities) > 0 {
				break
			}
		}
	}

	if result != nil && len(result.Entities) > 0 {
		oldestEntity := result.Entities[0]
		oldestPartitionKey := oldestEntity.PartitionKey
		log.Infof("Oldest partition key in '%s' table is %s (%s)", d.tableName, oldestPartitionKey, util.TimeFromTicksAscendingWithLeadingZero(oldestPartitionKey))
		return oldestPartitionKey, nil
	}

	return "", errors.New("Oldest record not found")
}

func (d *DefaultTablePurger) periodQueryOptionsGenerator(done <-chan interface{}, start, end time.Time) <-chan *storage.QueryOptions {
	queryOptionsStream := make(chan *storage.QueryOptions)
	go func() {
		defer close(queryOptionsStream)
		from := start
		to := end
		log.Debugf("Creating queryOptions: from %s to %s", from, to)
		fromTicks := util.TicksAscendingWithLeadingZero(util.TicksFromTime(from))
		toTicks := util.TicksAscendingWithLeadingZero(util.TicksFromTime(to))
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
