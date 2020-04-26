package purger

import (
	"github.com/rcrowley/go-metrics"
)

// Metrics contains metrics for azp.
type Metrics struct {
	metricsRegistry metrics.Registry
}

// NewMetrics dafg
func NewMetrics() *Metrics {
	return &Metrics{
		metricsRegistry: metrics.NewRegistry(),
	}
}
