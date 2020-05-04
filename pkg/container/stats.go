package container

import (
	"fmt"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/dustin/go-humanize"
)

// StatsGatherer simple implementation
type StatsGatherer struct {
	blobService *storage.BlobStorageClient
}

// NewStatsGatherer creates a new StatsGatherer
func NewStatsGatherer(accountName, accountKey string) (*StatsGatherer, error) {
	client, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	blobService := client.GetBlobService()
	return &StatsGatherer{blobService: &blobService}, nil
}

func (c *StatsGatherer) listContainers() ([]storage.Container, error) {
	log.Debug("Listing containers")
	containers := make([]storage.Container, 0)
	listParams := storage.ListContainersParameters{}
	containerListresponse, err := c.blobService.ListContainers(listParams)
	if err != nil {
		return nil, err
	}
	containers = append(containers, containerListresponse.Containers...)
	for containerListresponse != nil && containerListresponse.NextMarker != "" {
		listParams = storage.ListContainersParameters{Marker: containerListresponse.NextMarker}
		containerListresponse, err = c.blobService.ListContainers(listParams)
		containers = append(containers, containerListresponse.Containers...)
	}
	log.Debugf("Found %d container(s)", len(containers))
	return containers, nil
}

type callback func(blob storage.Blob)

func (c *StatsGatherer) forEachBlobInContainer(container *storage.Container, cb callback) error {
	listParams := storage.ListBlobsParameters{}
	response, err := container.ListBlobs(listParams)
	if err != nil {
		log.Errorf("Error listing blobs for %s. %s", container.Name, err)
		return err
	}
	for _, blob := range response.Blobs {
		cb(blob)
	}

	for response.NextMarker != "" {
		listParams = storage.ListBlobsParameters{Marker: response.NextMarker}
		response, err = container.ListBlobs(listParams)
		if err != nil {
			log.Errorf("Error retrieving marker %s for %s. %s", response.NextMarker, container.Name, err)
			return err
		}
		for _, blob := range response.Blobs {
			cb(blob)
		}
	}
	return nil
}

func (c *StatsGatherer) computeStats(container *storage.Container) (Stats, error) {
	log.Infof("Computing stats for %s", container.Name)
	ct := Stats{Name: container.Name}
	totalSize := int64(0)
	blobCount := int64(0)
	sizer := func(blob storage.Blob) {
		totalSize += blob.Properties.ContentLength
		blobCount++
		if blobCount%10000 == 0 {
			log.Debugf("Visited %s blobs in %s = %s", humanize.Comma(blobCount), container.Name, humanize.Bytes(uint64(totalSize)))
		}

	}
	err := c.forEachBlobInContainer(container, sizer)
	ct.BlobCount = blobCount
	ct.Size = uint64(totalSize)
	if err != nil {
		return ct, err
	}
	log.Info(ct)
	return ct, nil
}

// Stats holds the computed stats
type Stats struct {
	Name      string
	Size      uint64
	BlobCount int64
	Oldest    time.Time
	Newest    time.Time
}

func (c Stats) String() string {
	return fmt.Sprintf("%s contains %s blob(s) using a total of %s", c.Name, humanize.Comma(c.BlobCount), humanize.Bytes(c.Size))
}

// GatherStatistics Compute stats for all containers
func (c *StatsGatherer) GatherStatistics() ([]Stats, error) {
	numWorkers := runtime.NumCPU() * 2
	done := make(chan interface{})
	defer close(done)

	containerSlice, _ := c.listContainers()
	numJobs := len(containerSlice)
	jobs := make(chan storage.Container, numJobs)
	results := make(chan Stats, numJobs)

	for w := 1; w <= numWorkers; w++ {
		go func(id int, done chan interface{}, jobs chan storage.Container, results chan Stats) {
			for j := range jobs {
				log.Debugf("Worker %d started job %s", id, j.Name)
				start := time.Now().UTC()
				ct, _ := c.computeStats(&j)
				log.Debugf("Worker %d finished job %s in %s", id, j.Name, time.Since(start))
				select {
				case <-done:
					return
				case results <- ct:
				}
			}
		}(w, done, jobs, results)
	}

	for _, container := range containerSlice {
		select {
		case <-done:
			break
		case jobs <- container:
		}
	}
	close(jobs)

	containers := make([]Stats, numJobs)
	for result := range results {
		containers = append(containers, result)
	}

	return containers, nil
}
