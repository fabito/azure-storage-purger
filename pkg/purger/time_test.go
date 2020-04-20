package purger

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
	actual := timeFromTicksAscendingWithLeadingZero("0636603290790000000")
	assert.Equal(t, expected, actual)
}

func TestTimeFromTicksAscendingWithLeadingZero2(t *testing.T) {
	expected := time.Date(2020, 4, 19, 23, 13, 25, 4987531, time.UTC)
	actual := timeFromTicksAscendingWithLeadingZero("0637229348054987531")
	assert.Equal(t, expected, actual)
}
