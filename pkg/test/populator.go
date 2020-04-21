package test

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/fabito/azure-storage-purger/pkg/purger"

	"github.com/Azure/azure-sdk-for-go/storage"
)

func dates(start, end time.Time) chan time.Time {
	yield := make(chan time.Time)
	go func() {
		defer close(yield)
		for d := start; d.After(end) == false; d = d.AddDate(0, 0, 1) {
			yield <- d
		}
		// for y := start; y.Year() <= end.Year(); y = y.AddDate(1, 0, 0) {
		// 	for m := y; m.Year() == y.Year(); m = m.AddDate(0, 1, 0) {
		// 		for d := m; m.Year() == y.Year(); m = m.AddDate(0, 1, 0) {

		// 			yield <- m
		// 		}
		// 	}
		// }
	}()
	return yield
}

type partition struct {
	key      string
	entities []*storage.Entity
}

func partitions(table *storage.Table, maxNumberOfEntitiesPerPartition int, dates chan time.Time) chan *partition {
	yield := make(chan *partition)
	rand.Seed(time.Now().UnixNano())
	min := 1
	max := maxNumberOfEntitiesPerPartition
	go func() {
		defer close(yield)
		for m := range dates {
			partitionKey := purger.TicksAscendingWithLeadingZero(purger.TicksFromTime(m))
			entitiesPerPartitionCount := rand.Intn(max-min+1) + min
			p := &partition{
				key:      partitionKey,
				entities: make([]*storage.Entity, entitiesPerPartitionCount),
			}
			log.Printf("Adding %d to %s", entitiesPerPartitionCount, partitionKey)
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

	tableService := client.GetTableService()
	table := tableService.GetTableReference(tableName)

	if err := table.Get(5, storage.MinimalMetadata); err != nil {
		options := &storage.TableOptions{}
		err := table.Create(5, storage.MinimalMetadata, options)
		if err != nil {
			return nil, err
		}
	}

	return table, nil
}

// PopulateTable populates table with dummy test data
func PopulateTable(storageAccountName, storageAccountKey, tableName string, maxNumberOfEntitiesPerPartition int) error {
	table, err := createTable(storageAccountName, storageAccountKey, tableName)

	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	start := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Now().UTC()

	for batch := range batches(table, partitions(table, maxNumberOfEntitiesPerPartition, dates(start, end))) {
		wg.Add(1)
		go func(batch2 *storage.TableBatch) {
			batch2.ExecuteBatch()
			wg.Done()
		}(batch)
	}
	wg.Wait()
	return nil
}
