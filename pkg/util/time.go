package util

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const ticksAtEpock int64 = 621355968000000000
const ticksPerMillisecond int64 = 10000

// Period represet a period iof time
type Period struct {
	Start time.Time
	End   time.Time
}

//NewPeriod creates a new period instance
func NewPeriod(start, end time.Time) (*Period, error) {
	if start.After(end) {
		return nil, errors.New("End before start")
	}
	period := &Period{
		Start: start,
		End:   end,
	}
	return period, nil
}

// Duration the Periods's time.Duration
func (p *Period) Duration() time.Duration {
	return p.End.Sub(p.Start)
}

// SplitsFrom dsfg
func (p *Period) SplitsFrom(numSplits int) []Period {
	duration := p.End.Sub(p.Start).Milliseconds()
	if duration == 0 {
		return make([]Period, 0)
	}
	splits := make([]Period, numSplits)
	segmentLength := duration / int64(numSplits)
	step := time.Duration(segmentLength) * time.Millisecond
	s := p.Start
	for i := 1; i <= numSplits; i++ {
		var e time.Time
		if i == numSplits {
			e = p.End
		} else {
			e = s.Add(step)
		}
		splits[i-1] = Period{Start: s, End: e}
		s = e.Add(1 * time.Millisecond)
	}
	return splits
}

func (p *Period) Split(duration time.Duration) []Period {
	totalDuration := p.Duration()
	numSplits := totalDuration.Milliseconds() / duration.Milliseconds()
	return p.SplitsFrom(int(numSplits))
}

func (p Period) String() string {
	return fmt.Sprintf("%s -> %s (%s)", p.Start.Format(time.RFC3339), p.End.Format(time.RFC3339), p.Duration())
}

func LogPeriods(splits []Period) {
	for index, period := range splits {
		log.Infof("#%d: %s", index, period)
	}
}

func rightPad2Len(s string, padStr string, overallLen int) string {
	var padCountInt int
	padCountInt = 1 + ((overallLen - len(padStr)) / len(padStr))
	var retStr = s + strings.Repeat(padStr, padCountInt)
	return retStr[:overallLen]
}

// GetMaximumPartitionKeyToDelete TicksAscendingWithLeadingZero
func GetMaximumPartitionKeyToDelete(purgeRecordsOlderThanDays int) string {
	today := time.Now().UTC()
	then := today.AddDate(0, 0, -1*purgeRecordsOlderThanDays)
	then = time.Date(then.Year(), then.Month(), then.Day(), 0, 0, 0, 0, time.UTC)
	ticks := TicksFromTime(then)
	return TicksAscendingWithLeadingZero(ticks)
}

// TicksAscendingWithLeadingZero asdf
func TicksAscendingWithLeadingZero(ticks int64) string {
	s := fmt.Sprint("0", strconv.FormatInt(ticks, 10))
	ticksStr := rightPad2Len(s, "0", 19)
	return ticksStr
}

func TimeFromTicksAscendingWithLeadingZero(ticksStr string) time.Time {
	ticks, _ := strconv.ParseInt(ticksStr, 10, 64)
	return TimeFromTicks(ticks)
}

// TimeFromTicks convert ticks to time
func TimeFromTicks(ticks int64) time.Time {
	base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return time.Unix(ticks/10000000+base, ticks%10000000).UTC()
}

// TicksFromTime2 converts time to ticks
func TicksFromTime2(t time.Time) int64 {
	ticks := t.UTC().UnixNano() / 100
	return ticks + ticksAtEpock
}

// TicksFromTime converts time to ticks
func TicksFromTime(t time.Time) int64 {
	millis := t.UTC().UnixNano() / 1000000
	return (millis * ticksPerMillisecond) + ticksAtEpock
}
