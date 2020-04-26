package metrics

import (
	"fmt"
	"strings"
	"time"

	"github.com/rcrowley/go-metrics"

	log "github.com/sirupsen/logrus"
)

// Metrics contains metrics for azp.
type Metrics struct {
	metricsRegistry metrics.Registry
}

const (
	tableBatchTotal        = "table_batch_total"
	tableBatchSuccessTotal = "table_batch_success_total"
	tableBatchFailureTotal = "table_batch_failure_total"
	tableBatchDuration     = "table_batch_duration"
)

// NewMetrics dafg
func NewMetrics() *Metrics {
	metrics.Register(tableBatchTotal, metrics.NewCounter())
	metrics.Register(tableBatchSuccessTotal, metrics.NewCounter())
	metrics.Register(tableBatchFailureTotal, metrics.NewCounter())
	metrics.Register(tableBatchDuration, metrics.NewTimer())

	return &Metrics{
		metricsRegistry: metrics.DefaultRegistry,
	}
}

// RegisterTableBatchAttempt
func (m *Metrics) RegisterTableBatchAttempt() {
	if c, ok := m.metricsRegistry.Get(tableBatchTotal).(metrics.Counter); ok {
		c.Inc(1)
	}
}

// RegisterTableBatchFailed
func (m *Metrics) RegisterTableBatchFailed() {
	if c, ok := m.metricsRegistry.Get(tableBatchFailureTotal).(metrics.Counter); ok {
		c.Inc(1)
	}
}

// RegisterTableBatchSuccess
func (m *Metrics) RegisterTableBatchSuccess() {
	if c, ok := m.metricsRegistry.Get(tableBatchSuccessTotal).(metrics.Counter); ok {
		c.Inc(1)
	}
}

// RegisterTableBatchDurationSince updates duration since start time
func (m *Metrics) RegisterTableBatchDurationSince(start time.Time) {
	if c, ok := m.metricsRegistry.Get(tableBatchDuration).(metrics.Timer); ok {
		c.UpdateSince(start)
	}
}

func (m Metrics) String() string {
	scale := time.Millisecond
	du := float64(scale)
	duSuffix := scale.String()[1:]
	var b1 strings.Builder

	m.metricsRegistry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			b1.WriteString(fmt.Sprintf("counter %s\n", name))
			b1.WriteString(fmt.Sprintf("  count:       %9d\n", metric.Count()))
		case metrics.Gauge:
			b1.WriteString(fmt.Sprintf("gauge %s\n", name))
			b1.WriteString(fmt.Sprintf("  value:       %9d\n", metric.Value()))
		case metrics.GaugeFloat64:
			b1.WriteString(fmt.Sprintf("gauge %s\n", name))
			b1.WriteString(fmt.Sprintf("  value:       %f\n", metric.Value()))
		case metrics.Healthcheck:
			metric.Check()
			b1.WriteString(fmt.Sprintf("healthcheck %s\n", name))
			b1.WriteString(fmt.Sprintf("  error:       %v\n", metric.Error()))
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			b1.WriteString(fmt.Sprintf("histogram %s\n", name))
			b1.WriteString(fmt.Sprintf("  count:       %9d\n", h.Count()))
			b1.WriteString(fmt.Sprintf("  min:         %9d\n", h.Min()))
			b1.WriteString(fmt.Sprintf("  max:         %9d\n", h.Max()))
			b1.WriteString(fmt.Sprintf("  mean:        %12.2f\n", h.Mean()))
			b1.WriteString(fmt.Sprintf("  stddev:      %12.2f\n", h.StdDev()))
			b1.WriteString(fmt.Sprintf("  median:      %12.2f\n", ps[0]))
			b1.WriteString(fmt.Sprintf("  75%%:         %12.2f\n", ps[1]))
			b1.WriteString(fmt.Sprintf("  95%%:         %12.2f\n", ps[2]))
			b1.WriteString(fmt.Sprintf("  99%%:         %12.2f\n", ps[3]))
			b1.WriteString(fmt.Sprintf("  99.9%%:       %12.2f\n", ps[4]))
		case metrics.Meter:
			m := metric.Snapshot()
			b1.WriteString(fmt.Sprintf("meter %s\n", name))
			b1.WriteString(fmt.Sprintf("  count:       %9d\n", m.Count()))
			b1.WriteString(fmt.Sprintf("  1-min rate:  %12.2f\n", m.Rate1()))
			b1.WriteString(fmt.Sprintf("  5-min rate:  %12.2f\n", m.Rate5()))
			b1.WriteString(fmt.Sprintf("  15-min rate: %12.2f\n", m.Rate15()))
			b1.WriteString(fmt.Sprintf("  mean rate:   %12.2f\n", m.RateMean()))
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			b1.WriteString(fmt.Sprintf("timer %s\n", name))
			b1.WriteString(fmt.Sprintf("  count:       %9d\n", t.Count()))
			b1.WriteString(fmt.Sprintf("  min:         %12.2f%s\n", float64(t.Min())/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  max:         %12.2f%s\n", float64(t.Max())/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  mean:        %12.2f%s\n", t.Mean()/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  stddev:      %12.2f%s\n", t.StdDev()/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  median:      %12.2f%s\n", ps[0]/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  75%%:         %12.2f%s\n", ps[1]/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  95%%:         %12.2f%s\n", ps[2]/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  99%%:         %12.2f%s\n", ps[3]/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  99.9%%:       %12.2f%s\n", ps[4]/du, duSuffix))
			b1.WriteString(fmt.Sprintf("  1-min rate:  %12.2f\n", t.Rate1()))
			b1.WriteString(fmt.Sprintf("  5-min rate:  %12.2f\n", t.Rate5()))
			b1.WriteString(fmt.Sprintf("  15-min rate: %12.2f\n", t.Rate15()))
			b1.WriteString(fmt.Sprintf("  mean rate:   %12.2f\n", t.RateMean()))
		}
	})
	return b1.String()
}

func (m *Metrics) Log() {
	metrics.Log(m.metricsRegistry, 5*time.Second, log.StandardLogger())
}
