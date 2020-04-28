package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 11 Dec 2017 09:04:39.425194   0636485798400000000
// 04 Dec 2017 01:03:58.182063   0636479461800000000
// 21 Nov 2017 09:12:02.415054   2018-10-22T06:15:25.3970539Z    0636468523200000000
// 21 Nov 2017 09:12:15.156881   2018-10-22T06:15:25.3970539Z    0636468523200000000
// 22 Mar 2018 09:39:54.167379   2018-10-22T06:20:17.991Z        0636573083400000000
// 26 Apr 2018 08:44:39.280577   2018-10-22T06:21:19.2223227Z    0636603290400000000
// 30 Jan 2018 02:33:57.920976   2018-10-22T06:19:31.315974Z     0636528763800000000
func TestTicksFromTime(t *testing.T) {
	base := time.Date(2018, 4, 26, 8, 44, 39, 280577, time.UTC)
	ticks := TicksFromTime(base)
	assert.Equal(t, int64(636603290790000000), ticks)
}

func TestTicksFromTime2(t *testing.T) {
	base := time.Date(2020, 4, 19, 23, 13, 25, 4987531, time.UTC)
	ticks := TicksFromTime2(base)
	// assert.Equal(t, int64(637229348054987531), ticks)
	assert.Equal(t, int64(637229348050049875), ticks)
}

func TestTicksAscendingWithLeadingZero(t *testing.T) {
	base := time.Date(2018, 4, 26, 8, 44, 39, 280577, time.UTC)
	ticks := TicksAscendingWithLeadingZero(TicksFromTime(base))
	assert.Equal(t, "0636603290790000000", ticks)
}

func TestTimeFromTicksAscendingWithLeadingZero(t *testing.T) {
	expected := time.Date(2018, 4, 26, 8, 44, 39, 0, time.UTC)
	actual := TimeFromTicksAscendingWithLeadingZero("0636603290790000000")
	assert.Equal(t, expected, actual)
}

func TestTimeFromTicksAscendingWithLeadingZero2(t *testing.T) {
	expected := time.Date(2020, 4, 19, 23, 13, 25, 4987531, time.UTC)
	actual := TimeFromTicksAscendingWithLeadingZero("0637229348054987531")
	assert.Equal(t, expected, actual)
}

func TestSplitPeriodsEven(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

	p := Period{Start: start, End: end}
	splits := p.SplitsFrom(2)

	assert.Equal(t, 2, len(splits))
	assert.Equal(t, start, splits[0].Start)
	assert.True(t, splits[0].End.After(splits[0].Start))
	assert.True(t, splits[0].End.Before(splits[1].Start))
	assert.True(t, splits[1].End.After(splits[1].Start))
	assert.Equal(t, end, splits[1].End)

	LogPeriods(splits)
}

func TestSplitPeriodsOdd(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

	p := Period{Start: start, End: end}
	splits := p.SplitsFrom(3)

	assert.Equal(t, 3, len(splits))
	assert.Equal(t, start, splits[0].Start)
	assert.True(t, splits[0].End.After(splits[0].Start))
	assert.True(t, splits[0].End.Before(splits[1].Start))
	assert.True(t, splits[1].End.After(splits[1].Start))
	assert.Equal(t, end, splits[2].End)

	LogPeriods(splits)
}

func TestSplitPeriodsSpecific(t *testing.T) {
	// 2018-07-11 00:00:00 +0000 UTC
	// 2018-08-11 13:17:09.00962 +0000 UTC

	start := time.Date(2018, 7, 11, 0, 0, 0, 0, time.UTC)
	end := time.Date(2018, 8, 11, 13, 17, 9, 9620000, time.UTC)

	p := Period{Start: start, End: end}
	splits := p.SplitsFrom(16)

	assert.Equal(t, 16, len(splits))
	assert.Equal(t, start, splits[0].Start)
	assert.True(t, splits[0].End.After(splits[0].Start))
	assert.True(t, splits[0].End.Before(splits[1].Start))
	assert.True(t, splits[1].End.After(splits[1].Start))
	assert.Equal(t, end, splits[15].End)

	LogPeriods(splits)
}

func TestSplitPeriodsOneDay(t *testing.T) {
	// 2018-07-11 00:00:00 +0000 UTC
	// 2018-07-12 20:58:32.00879 +0000 UTC

	start := time.Date(2018, 7, 11, 0, 0, 0, 0, time.UTC)
	end := time.Date(2018, 7, 12, 20, 58, 32, 8790000, time.UTC)

	p := Period{Start: start, End: end}
	splits := p.SplitsFrom(16)

	assert.Equal(t, 16, len(splits))
	assert.Equal(t, start, splits[0].Start)
	assert.True(t, splits[0].End.After(splits[0].Start))
	assert.True(t, splits[0].End.Before(splits[1].Start))
	assert.True(t, splits[1].End.After(splits[1].Start))
	assert.Equal(t, end, splits[15].End)

	LogPeriods(splits)
}

func TestParsePeriod(t *testing.T) {
	start := time.Date(2018, 7, 11, 0, 0, 0, 0, time.UTC)
	end := time.Date(2018, 7, 12, 23, 59, 59, 0, time.UTC)
	expected, _ := NewPeriod(start, end)
	actual, err := ParsePeriod("2018-07-11", "2018-07-12")
	if assert.NoError(t, err) {
		assert.Equal(t, expected, actual)
	}
}
