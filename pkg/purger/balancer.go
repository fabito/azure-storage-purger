package purger

import (
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/fabito/azure-storage-purger/pkg/util"
	log "github.com/sirupsen/logrus"
)

type QueryOptionsGenerator interface {
	Generate(done <-chan interface{}) <-chan *storage.QueryOptions
}

type DefaultQueryOptionsGenerator struct {
	period *util.Period
}

func NewDefaultQueryOptionsGenerator(start, end time.Time) (QueryOptionsGenerator, error) {
	period, err := util.NewPeriod(start, end)
	if err != nil {
		return &DefaultQueryOptionsGenerator{
			period: period,
		}, nil
	}
	return nil, err
}

func (q *DefaultQueryOptionsGenerator) Generate(done <-chan interface{}) <-chan *storage.QueryOptions {
	queryOptionsStream := make(chan *storage.QueryOptions)
	go func() {
		defer close(queryOptionsStream)
		from := q.period.Start
		to := q.period.End
		log.Infof("Creating queryOptions: from %s to %s", from, to)
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

type FixedDurationQueryOptionsGenerator struct {
	period   *util.Period
	duration time.Duration
}

func (q *FixedDurationQueryOptionsGenerator) Generate(done <-chan interface{}) <-chan *storage.QueryOptions {
	queryOptionsStream := make(chan *storage.QueryOptions)
	go func() {
		defer close(queryOptionsStream)
		for _, period := range q.period.Split(q.duration) {
			from := period.Start
			to := period.End
			log.Infof("Creating queryOptions: from %s to %s", from, to)
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
		}
	}()
	return queryOptionsStream
}

// QueryOptionsGeneratorFunc is a method that implements the Sender interface.
type QueryOptionsGeneratorFunc func(done <-chan interface{}) <-chan *storage.QueryOptions

// Generate implements the QueryOptionsGenerator interface on QueryOptionsGeneratorFunc.
func (sf QueryOptionsGeneratorFunc) Generate(done <-chan interface{}) <-chan *storage.QueryOptions {
	return sf(done)
}

func SenderWithLogging(duration time.Time) QueryOptionsGenerator {
	return QueryOptionsGeneratorFunc(func(done <-chan interface{}) <-chan *storage.QueryOptions {
		queryOptionsStream := make(chan *storage.QueryOptions)
		return queryOptionsStream
	})
}
