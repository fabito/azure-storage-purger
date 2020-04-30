package container

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/dustin/go-humanize"
)

type ContainerStatsGatherer struct {
	blobService *storage.BlobStorageClient
}

func NewContainerStatsGatherer(accountName, accountKey string) (*ContainerStatsGatherer, error) {
	client, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	blobService := client.GetBlobService()
	return &ContainerStatsGatherer{blobService: &blobService}, nil
}

func (c *ContainerStatsGatherer) listContainers() ([]storage.Container, error) {
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

func (c *ContainerStatsGatherer) forEachBlobInContainer(container *storage.Container, cb callback) error {
	listParams := storage.ListBlobsParameters{}
	response, err := container.ListBlobs(listParams)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, blob := range response.Blobs {
		cb(blob)
	}

	for response.NextMarker != "" {
		listParams = storage.ListBlobsParameters{Marker: response.NextMarker}
		response, err = container.ListBlobs(listParams)
		if err != nil {
			log.Error(err)
			return err
		}
		for _, blob := range response.Blobs {
			cb(blob)
		}
	}
	return nil
}

func (c *ContainerStatsGatherer) computeContainerSize(container *storage.Container) (int64, error) {
	log.Infof("Computing size for %s", container.Name)
	totalSize := int64(0)
	sizer := func(blob storage.Blob) {
		totalSize += blob.Properties.ContentLength
	}
	err := c.forEachBlobInContainer(container, sizer)
	if err != nil {
		return totalSize, err
	}
	return totalSize, nil
}

type ContainerStats struct {
	Name      string
	Size      uint64
	BlobCount uint64
	Oldest    time.Time
	Newest    time.Time
}

func (c ContainerStats) string() string {
	return fmt.Sprintf("%s %s", c.Name, humanize.Bytes(c.Size))
}

func (c *ContainerStatsGatherer) GetContainerSizes() ([]ContainerStats, error) {
	cotainerList, _ := c.listContainers()
	containers := make([]ContainerStats, len(cotainerList))
	for _, container := range cotainerList {
		containerSize, _ := c.computeContainerSize(&container)
		ct := ContainerStats{Name: container.Name, Size: uint64(containerSize)}
		containers = append(containers, ct)
	}
	return containers, nil
}
