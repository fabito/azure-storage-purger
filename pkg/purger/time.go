package purger

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const ticksAtEpock int64 = 621355968000000000
const ticksPerMillisecond int64 = 10000

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
	ticks := TicksFromTime(then)
	return ticksAscendingWithLeadingZero(ticks)
}

func ticksAscendingWithLeadingZero(ticks int64) string {
	s := fmt.Sprint("0", strconv.FormatInt(ticks, 10))
	ticksStr := rightPad2Len(s, "0", 19)
	return ticksStr
}

// TimeFromTicks convert ticks to time
func TimeFromTicks(ticks int64) time.Time {
	base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return time.Unix(ticks/10000000+base, ticks%10000000).UTC()
}

// TicksFromTime converts time to ticks
func TicksFromTime(t time.Time) int64 {
	millis := t.UTC().UnixNano() / 1000000
	return (millis * ticksPerMillisecond) + ticksAtEpock
}
