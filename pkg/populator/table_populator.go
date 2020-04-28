package populator

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/fabito/azure-storage-purger/pkg/metrics"
	"github.com/fabito/azure-storage-purger/pkg/util"
	"github.com/fabito/azure-storage-purger/pkg/work"
	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
)

func dates(start, end time.Time) chan time.Time {
	yield := make(chan time.Time)
	log.Infof("Generating data from %s to %s", start, end)
	go func() {
		defer close(yield)
		for d := start; d.After(end) == false; d = d.AddDate(0, 0, 1) {
			yield <- d
		}
	}()
	return yield
}

type partition struct {
	key      string
	entities []*storage.Entity
}

func randomPartitions(table *storage.Table, maxNumberOfEntitiesPerPartition int, dates chan time.Time) chan *partition {
	yield := make(chan *partition)
	rand.Seed(time.Now().UnixNano())
	min := 1
	max := maxNumberOfEntitiesPerPartition
	go func() {
		defer close(yield)
		for m := range dates {
			partitionKey := util.TicksAscendingWithLeadingZero(util.TicksFromTime(m))
			entitiesPerPartitionCount := rand.Intn(max-min+1) + min
			p := &partition{
				key:      partitionKey,
				entities: make([]*storage.Entity, entitiesPerPartitionCount),
			}
			log.Debugf("Adding %d to %s ()", entitiesPerPartitionCount, partitionKey)
			for i := 0; i < entitiesPerPartitionCount; i++ {
				e := table.GetEntityReference(partitionKey, strconv.Itoa(i+1))
				props := map[string]interface{}{
					"CreatedOn": m,
				}
				e.Properties = props
				p.entities[i] = e
			}
			yield <- p
		}
	}()
	return yield
}

func partitions(table *storage.Table, maxNumberOfEntitiesPerPartition int, dates chan time.Time) chan *partition {
	yield := make(chan *partition)
	go func() {
		defer close(yield)
		for m := range dates {
			partitionKey := util.TicksAscendingWithLeadingZero(util.TicksFromTime(m))
			p := &partition{
				key:      partitionKey,
				entities: make([]*storage.Entity, maxNumberOfEntitiesPerPartition),
			}
			log.Debugf("Adding %d to %s ()", maxNumberOfEntitiesPerPartition, partitionKey)
			for i := 0; i < maxNumberOfEntitiesPerPartition; i++ {
				e := table.GetEntityReference(partitionKey, strconv.Itoa(i+1))
				props := map[string]interface{}{
					"CreatedOn": m,
				}
				e.Properties = props
				p.entities[i] = e
			}
			yield <- p
		}
	}()
	return yield
}

func batchesFromPartition(table *storage.Table, p *partition) []*storage.TableBatch {
	chunkSize := 100
	entities := p.entities
	count := len(entities)
	batches := make([]*storage.TableBatch, 0)
	for i := 0; i < count; i += chunkSize {
		end := i + chunkSize
		if end > count {
			end = count
		}
		tableBatch := table.NewBatch()
		for _, entity := range entities[i:end] {
			tableBatch.InsertOrMergeEntityByForce(entity)
		}
		batches = append(batches, tableBatch)
	}
	return batches
}

func batches(table *storage.Table, partitions chan *partition) chan *storage.TableBatch {
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
				tableBatch := table.NewBatch()
				for _, entity := range entities[i:end] {
					tableBatch.InsertOrMergeEntityByForce(entity)
				}
				yield <- tableBatch
			}
		}
	}()

	return yield
}

func createTable(storageAccountName, storageAccountKey, tableName string) (*storage.Table, error) {
	client, err := storage.NewBasicClient(storageAccountName, storageAccountKey)
	if err != nil {
		return nil, err
	}

	if log.IsLevelEnabled(log.TraceLevel) {
		client.Sender = util.SenderWithLogging(client.Sender)
	}

	tableService := client.GetTableService()
	table := tableService.GetTableReference(tableName)

	if err := table.Get(5, storage.MinimalMetadata); err != nil {
		log.Infof("Table %s doesn't exist. Creating...", tableName)
		options := &storage.TableOptions{}
		err := table.Create(5, storage.MinimalMetadata, options)
		if err != nil {
			log.Errorf("Error creating table %s", tableName)
			return nil, err
		}
	}

	return table, nil
}

type tableBatchRunner struct {
	batch   *storage.TableBatch
	metrics *metrics.Metrics
}

// Task implements the Worker interface.
func (t *tableBatchRunner) Task() {
	t.metrics.RegisterTableBatchAttempt()
	start := time.Now()
	err := t.batch.ExecuteBatch()
	if err != nil {
		t.metrics.RegisterTableBatchFailed()
		log.Error(err)
	} else {
		t.metrics.RegisterTableBatchDurationSince(start)
		t.metrics.RegisterTableBatchSuccess()
	}
}

type tablePartitionRunner struct {
	partition *partition
	metrics   *metrics.Metrics
	table     *storage.Table
}

func (t *tablePartitionRunner) Task() {
	for _, batch := range batchesFromPartition(t.table, t.partition) {
		t.metrics.RegisterTableBatchAttempt()
		start := time.Now()
		err := batch.ExecuteBatch()
		if err != nil {
			t.metrics.RegisterTableBatchFailed()
			log.Error(err)
		} else {
			t.metrics.RegisterTableBatchDurationSince(start)
			t.metrics.RegisterTableBatchSuccess()
		}
	}
}

// PopulateTable populates table with dummy test data
func PopulateTable(storageAccountName, storageAccountKey, tableName string, startDate, endDate time.Time, maxNumberOfEntitiesPerPartition, numWorkers int) error {
	table, err := createTable(storageAccountName, storageAccountKey, tableName)

	if err != nil {
		return err
	}

	metrics := metrics.NewMetrics()

	log.Infof("Start %s population.", tableName)

	p := work.New(numWorkers)
	var wg sync.WaitGroup

	go metrics.Log()

	for partition := range partitions(table, maxNumberOfEntitiesPerPartition, dates(startDate, endDate)) {
		wg.Add(1)
		job := tablePartitionRunner{metrics: metrics, partition: partition, table: table}
		go func() {
			p.Run(&job)
			wg.Done()
		}()
	}
	wg.Wait()

	// Shutdown the work pool and wait for all existing work
	// to be completed.
	p.Shutdown()
	return nil
}
