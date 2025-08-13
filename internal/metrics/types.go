package metrics

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// MetricType represents different types of metrics
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
	MetricTypeTimer     MetricType = "timer"
)

// Metric represents a generic metric interface
type Metric interface {
	Name() string
	Type() MetricType
	Value() interface{}
	Labels() map[string]string
	Reset()
	Clone() Metric
}

// Counter represents a monotonically increasing counter
type Counter interface {
	Metric
	Inc()
	Add(delta int64)
	Get() int64
}

// Gauge represents a value that can go up and down
type Gauge interface {
	Metric
	Set(value float64)
	Add(delta float64)
	Sub(delta float64)
	Get() float64
}

// Histogram represents a distribution of values
type Histogram interface {
	Metric
	Observe(value float64)
	GetBuckets() []HistogramBucket
	GetCount() int64
	GetSum() float64
}

// Timer represents a timer metric
type Timer interface {
	Metric
	Record(duration time.Duration)
	RecordSince(start time.Time)
	Time() func()
	GetCount() int64
	GetSum() time.Duration
	GetMean() time.Duration
	GetPercentiles() map[float64]time.Duration
}

// HistogramBucket represents a bucket in a histogram
type HistogramBucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      int64   `json:"count"`
}

// MetricRegistry manages a collection of metrics
type MetricRegistry interface {
	Counter(name string, labels map[string]string) *CounterImpl
	Gauge(name string, labels map[string]string) *GaugeImpl
	Histogram(name string, labels map[string]string, buckets []float64) *HistogramImpl
	Timer(name string, labels map[string]string) *TimerImpl
	GetMetric(name string, labels map[string]string) Metric
	GetAllMetrics() []Metric
	UnregisterMetric(name string, labels map[string]string)
	Clear()
}

// MetricExporter exports metrics to external systems
type MetricExporter interface {
	Export(metrics []Metric) error
	Close() error
}

// MetricCollector collects metrics from various sources
type MetricCollector interface {
	Collect() ([]Metric, error)
	Name() string
}

// MetricsConfig represents metrics configuration
type MetricsConfig struct {
	Enabled            bool          `json:"enabled"`
	CollectionInterval time.Duration `json:"collection_interval"`
	RetentionPeriod    time.Duration `json:"retention_period"`
	MaxMetrics         int           `json:"max_metrics"`
	EnableProfiling    bool          `json:"enable_profiling"`

	// Export configuration
	Exporters []ExporterConfig `json:"exporters"`

	// Custom buckets for histograms
	DefaultHistogramBuckets []float64 `json:"default_histogram_buckets"`

	// Labels to add to all metrics
	GlobalLabels map[string]string `json:"global_labels"`
}

// ExporterConfig represents exporter configuration
type ExporterConfig struct {
	Type     string                 `json:"type"`
	Endpoint string                 `json:"endpoint"`
	Interval time.Duration          `json:"interval"`
	Config   map[string]interface{} `json:"config"`
}

// DefaultMetricsConfig returns default metrics configuration
func DefaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		Enabled:            true,
		CollectionInterval: 10 * time.Second,
		RetentionPeriod:    1 * time.Hour,
		MaxMetrics:         10000,
		EnableProfiling:    false,
		DefaultHistogramBuckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
		},
		GlobalLabels: make(map[string]string),
	}
}

// CounterImpl implements Counter interface
type CounterImpl struct {
	name   string
	labels map[string]string
	value  int64
}

// NewCounterImpl creates a new counter
func NewCounterImpl(name string, labels map[string]string) *CounterImpl {
	return &CounterImpl{
		name:   name,
		labels: labels,
		value:  0,
	}
}

// Name returns the metric name
func (c *CounterImpl) Name() string {
	return c.name
}

// Type returns the metric type
func (c *CounterImpl) Type() MetricType {
	return MetricTypeCounter
}

// Value returns the current value
func (c *CounterImpl) Value() interface{} {
	return atomic.LoadInt64(&c.value)
}

// Labels returns the metric labels
func (c *CounterImpl) Labels() map[string]string {
	return c.labels
}

// Reset resets the counter to zero
func (c *CounterImpl) Reset() {
	atomic.StoreInt64(&c.value, 0)
}

// Clone creates a copy of the counter
func (c *CounterImpl) Clone() Metric {
	return &CounterImpl{
		name:   c.name,
		labels: c.labels,
		value:  atomic.LoadInt64(&c.value),
	}
}

// Inc increments the counter by 1
func (c *CounterImpl) Inc() {
	atomic.AddInt64(&c.value, 1)
}

// Add adds delta to the counter
func (c *CounterImpl) Add(delta int64) {
	atomic.AddInt64(&c.value, delta)
}

// Get returns the current counter value
func (c *CounterImpl) Get() int64 {
	return atomic.LoadInt64(&c.value)
}

// GaugeImpl implements Gauge interface
type GaugeImpl struct {
	name   string
	labels map[string]string
	value  uint64 // Using uint64 to store float64 bits atomically
	mu     sync.RWMutex
}

// NewGaugeImpl creates a new gauge
func NewGaugeImpl(name string, labels map[string]string) *GaugeImpl {
	return &GaugeImpl{
		name:   name,
		labels: labels,
		value:  0,
	}
}

// Name returns the metric name
func (g *GaugeImpl) Name() string {
	return g.name
}

// Type returns the metric type
func (g *GaugeImpl) Type() MetricType {
	return MetricTypeGauge
}

// Value returns the current value
func (g *GaugeImpl) Value() interface{} {
	return g.Get()
}

// Labels returns the metric labels
func (g *GaugeImpl) Labels() map[string]string {
	return g.labels
}

// Reset resets the gauge to zero
func (g *GaugeImpl) Reset() {
	g.Set(0)
}

// Clone creates a copy of the gauge
func (g *GaugeImpl) Clone() Metric {
	return &GaugeImpl{
		name:   g.name,
		labels: g.labels,
		value:  atomic.LoadUint64(&g.value),
	}
}

// Set sets the gauge value
func (g *GaugeImpl) Set(value float64) {
	atomic.StoreUint64(&g.value, floatToBits(value))
}

// Add adds delta to the gauge
func (g *GaugeImpl) Add(delta float64) {
	for {
		current := g.Get()
		new := current + delta
		if g.compareAndSwap(current, new) {
			break
		}
	}
}

// Sub subtracts delta from the gauge
func (g *GaugeImpl) Sub(delta float64) {
	g.Add(-delta)
}

// Get returns the current gauge value
func (g *GaugeImpl) Get() float64 {
	return bitsToFloat(atomic.LoadUint64(&g.value))
}

func (g *GaugeImpl) compareAndSwap(old, new float64) bool {
	return atomic.CompareAndSwapUint64(&g.value, floatToBits(old), floatToBits(new))
}

// HistogramImpl implements Histogram interface
type HistogramImpl struct {
	name    string
	labels  map[string]string
	buckets []HistogramBucket
	count   int64
	sum     uint64 // Using uint64 to store float64 bits atomically
	mu      sync.RWMutex
}

// NewHistogramImpl creates a new histogram
func NewHistogramImpl(name string, labels map[string]string, buckets []float64) *HistogramImpl {
	histBuckets := make([]HistogramBucket, len(buckets))
	for i, bound := range buckets {
		histBuckets[i] = HistogramBucket{
			UpperBound: bound,
			Count:      0,
		}
	}

	return &HistogramImpl{
		name:    name,
		labels:  labels,
		buckets: histBuckets,
		count:   0,
		sum:     0,
	}
}

// Name returns the metric name
func (h *HistogramImpl) Name() string {
	return h.name
}

// Type returns the metric type
func (h *HistogramImpl) Type() MetricType {
	return MetricTypeHistogram
}

// Value returns the histogram data
func (h *HistogramImpl) Value() interface{} {
	return map[string]interface{}{
		"buckets": h.GetBuckets(),
		"count":   h.GetCount(),
		"sum":     h.GetSum(),
	}
}

// Labels returns the metric labels
func (h *HistogramImpl) Labels() map[string]string {
	return h.labels
}

// Reset resets the histogram
func (h *HistogramImpl) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.buckets {
		h.buckets[i].Count = 0
	}
	atomic.StoreInt64(&h.count, 0)
	atomic.StoreUint64(&h.sum, 0)
}

// Clone creates a copy of the histogram
func (h *HistogramImpl) Clone() Metric {
	h.mu.RLock()
	defer h.mu.RUnlock()

	buckets := make([]HistogramBucket, len(h.buckets))
	copy(buckets, h.buckets)

	return &HistogramImpl{
		name:    h.name,
		labels:  h.labels,
		buckets: buckets,
		count:   atomic.LoadInt64(&h.count),
		sum:     atomic.LoadUint64(&h.sum),
	}
}

// Observe adds an observation to the histogram
func (h *HistogramImpl) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Find the appropriate bucket
	for i := range h.buckets {
		if value <= h.buckets[i].UpperBound {
			atomic.AddInt64(&h.buckets[i].Count, 1)
		}
	}

	atomic.AddInt64(&h.count, 1)

	// Add to sum atomically
	for {
		current := h.GetSum()
		new := current + value
		if atomic.CompareAndSwapUint64(&h.sum, floatToBits(current), floatToBits(new)) {
			break
		}
	}
}

// GetBuckets returns the histogram buckets
func (h *HistogramImpl) GetBuckets() []HistogramBucket {
	h.mu.RLock()
	defer h.mu.RUnlock()

	buckets := make([]HistogramBucket, len(h.buckets))
	for i, bucket := range h.buckets {
		buckets[i] = HistogramBucket{
			UpperBound: bucket.UpperBound,
			Count:      atomic.LoadInt64(&bucket.Count),
		}
	}

	return buckets
}

// GetCount returns the total count of observations
func (h *HistogramImpl) GetCount() int64 {
	return atomic.LoadInt64(&h.count)
}

// GetSum returns the sum of all observations
func (h *HistogramImpl) GetSum() float64 {
	return bitsToFloat(atomic.LoadUint64(&h.sum))
}

// TimerImpl implements Timer interface
type TimerImpl struct {
	name      string
	labels    map[string]string
	histogram *HistogramImpl
	count     int64
	sum       int64 // Nanoseconds
	mu        sync.RWMutex
	values    []time.Duration // For percentile calculation
}

// NewTimerImpl creates a new timer
func NewTimerImpl(name string, labels map[string]string) *TimerImpl {
	// Default buckets for timer (in seconds)
	buckets := []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
	}

	return &TimerImpl{
		name:      name,
		labels:    labels,
		histogram: NewHistogramImpl(name+"_histogram", labels, buckets),
		count:     0,
		sum:       0,
		values:    make([]time.Duration, 0, 1000), // Keep last 1000 values for percentiles
	}
}

// Name returns the metric name
func (t *TimerImpl) Name() string {
	return t.name
}

// Type returns the metric type
func (t *TimerImpl) Type() MetricType {
	return MetricTypeTimer
}

// Value returns the timer data
func (t *TimerImpl) Value() interface{} {
	return map[string]interface{}{
		"count":       t.GetCount(),
		"sum":         t.GetSum(),
		"mean":        t.GetMean(),
		"percentiles": t.GetPercentiles(),
	}
}

// Labels returns the metric labels
func (t *TimerImpl) Labels() map[string]string {
	return t.labels
}

// Reset resets the timer
func (t *TimerImpl) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	atomic.StoreInt64(&t.count, 0)
	atomic.StoreInt64(&t.sum, 0)
	t.values = t.values[:0]
	t.histogram.Reset()
}

// Clone creates a copy of the timer
func (t *TimerImpl) Clone() Metric {
	t.mu.RLock()
	defer t.mu.RUnlock()

	values := make([]time.Duration, len(t.values))
	copy(values, t.values)

	return &TimerImpl{
		name:      t.name,
		labels:    t.labels,
		histogram: t.histogram.Clone().(*HistogramImpl),
		count:     atomic.LoadInt64(&t.count),
		sum:       atomic.LoadInt64(&t.sum),
		values:    values,
	}
}

// Record records a duration
func (t *TimerImpl) Record(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	atomic.AddInt64(&t.count, 1)
	atomic.AddInt64(&t.sum, int64(duration))

	// Add to histogram (convert to seconds)
	t.histogram.Observe(duration.Seconds())

	// Keep recent values for percentile calculation
	t.values = append(t.values, duration)
	if len(t.values) > 1000 {
		t.values = t.values[1:]
	}
}

// RecordSince records the time since start
func (t *TimerImpl) RecordSince(start time.Time) {
	t.Record(time.Since(start))
}

// Time returns a function that records the time when called
func (t *TimerImpl) Time() func() {
	start := time.Now()
	return func() {
		t.RecordSince(start)
	}
}

// GetCount returns the total count
func (t *TimerImpl) GetCount() int64 {
	return atomic.LoadInt64(&t.count)
}

// GetSum returns the total sum
func (t *TimerImpl) GetSum() time.Duration {
	return time.Duration(atomic.LoadInt64(&t.sum))
}

// GetMean returns the mean duration
func (t *TimerImpl) GetMean() time.Duration {
	count := t.GetCount()
	if count == 0 {
		return 0
	}
	return time.Duration(atomic.LoadInt64(&t.sum) / count)
}

// GetPercentiles returns percentile values
func (t *TimerImpl) GetPercentiles() map[float64]time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.values) == 0 {
		return map[float64]time.Duration{}
	}

	// Sort values for percentile calculation
	sorted := make([]time.Duration, len(t.values))
	copy(sorted, t.values)

	// Simple insertion sort for small arrays
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	percentiles := map[float64]time.Duration{
		0.5:  sorted[int(float64(len(sorted))*0.5)],
		0.9:  sorted[int(float64(len(sorted))*0.9)],
		0.95: sorted[int(float64(len(sorted))*0.95)],
		0.99: sorted[int(float64(len(sorted))*0.99)],
	}

	return percentiles
}

// Helper functions for atomic float64 operations
func floatToBits(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

func bitsToFloat(b uint64) float64 {
	return *(*float64)(unsafe.Pointer(&b))
}
