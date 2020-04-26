package populator

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/fabito/azure-storage-purger/pkg/util"

	"github.com/Azure/azure-sdk-for-go/storage"
)

func getDates() []time.Time {
	r := make([]time.Time, 0)
	start := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	for y := start; y.Year() <= end.Year(); y = y.AddDate(1, 0, 0) {
		for m := y; m.Year() == y.Year(); m = m.AddDate(0, 1, 0) {
			r = append(r, m)
		}
	}
	return r
}

func getPartitions(table *storage.Table, dateList []time.Time) []*partition {
	yield := make([]*partition, 0)
	rand.Seed(time.Now().UnixNano())
	min := 1
	max := 150

	for _, m := range dateList {
		partitionKey := util.TicksAscendingWithLeadingZero(util.TicksFromTime(m))
		entitiesPerPartitionCount := rand.Intn(max-min+1) + min
		p := &partition{
			key:      partitionKey,
			entities: make([]*storage.Entity, entitiesPerPartitionCount),
		}
		log.Printf("Adding %d to %s", entitiesPerPartitionCount, partitionKey)
		for i := 0; i < entitiesPerPartitionCount; i++ {
			e := table.GetEntityReference(partitionKey, strconv.Itoa(i+1))
			e.TimeStamp = m
			p.entities[i] = e
		}
		yield = append(yield, p)
	}

	// log.Println(yield)

	return yield
}

func getBatches(table *storage.Table, partitions []*partition) []*storage.TableBatch {
	yield := make([]*storage.TableBatch, 0)
	chunkSize := 100

	for _, p := range partitions {
		entities := p.entities
		count := len(entities)
		for i := 0; i < count; i += chunkSize {
			end := i + chunkSize
			if end > count {
				end = count
			}
			tableBatch := table.NewBatch()
			for _, entity := range entities[i:end] {
				tableBatch.InsertEntity(entity)
			}
			yield = append(yield, tableBatch)
		}
	}

	return yield
}

// CreateAndPopulateTable create and populate storage table with test data
func CreateAndPopulateTable(storageAccountName, storageAccountKey, tableName string) error {
	table, _ := createTable(storageAccountName, storageAccountKey, tableName)

	for _, batch := range getBatches(table, getPartitions(table, getDates())) {
		batch.ExecuteBatch()
	}
	return nil
}
