package metrics

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// SystemMetricsCollector collects system-level metrics
type SystemMetricsCollector struct {
	name string
	mu   sync.RWMutex
}

// NewSystemMetricsCollector creates a new system metrics collector
func NewSystemMetricsCollector() *SystemMetricsCollector {
	return &SystemMetricsCollector{
		name: "system",
	}
}

// Name returns the collector name
func (smc *SystemMetricsCollector) Name() string {
	return smc.name
}

// Collect collects system metrics
func (smc *SystemMetricsCollector) Collect() ([]Metric, error) {
	var metrics []Metric

	// Memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Memory metrics
	metrics = append(metrics, NewGaugeImpl("go_memory_alloc_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.Alloc))

	metrics = append(metrics, NewGaugeImpl("go_memory_total_alloc_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.TotalAlloc))

	metrics = append(metrics, NewGaugeImpl("go_memory_sys_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.Sys))

	metrics = append(metrics, NewGaugeImpl("go_memory_heap_alloc_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.HeapAlloc))

	metrics = append(metrics, NewGaugeImpl("go_memory_heap_sys_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.HeapSys))

	metrics = append(metrics, NewGaugeImpl("go_memory_heap_inuse_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.HeapInuse))

	metrics = append(metrics, NewGaugeImpl("go_memory_stack_inuse_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.StackInuse))

	metrics = append(metrics, NewGaugeImpl("go_memory_stack_sys_bytes", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.StackSys))

	// GC metrics
	metrics = append(metrics, NewCounterImpl("go_gc_runs_total", nil))
	metrics[len(metrics)-1].(*CounterImpl).Add(int64(memStats.NumGC))

	metrics = append(metrics, NewGaugeImpl("go_gc_pause_seconds", nil))
	if memStats.NumGC > 0 {
		metrics[len(metrics)-1].(*GaugeImpl).Set(float64(memStats.PauseNs[(memStats.NumGC+255)%256]) / 1e9)
	}

	// Goroutine metrics
	metrics = append(metrics, NewGaugeImpl("go_goroutines", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(runtime.NumGoroutine()))

	// CPU metrics
	metrics = append(metrics, NewGaugeImpl("go_threads", nil))
	metrics[len(metrics)-1].(*GaugeImpl).Set(float64(runtime.GOMAXPROCS(0)))

	return metrics, nil
}

// StorageMetricsCollector collects storage-related metrics
type StorageMetricsCollector struct {
	name       string
	getMetrics func() map[string]interface{}
	mu         sync.RWMutex
}

// NewStorageMetricsCollector creates a new storage metrics collector
func NewStorageMetricsCollector(getMetrics func() map[string]interface{}) *StorageMetricsCollector {
	return &StorageMetricsCollector{
		name:       "storage",
		getMetrics: getMetrics,
	}
}

// Name returns the collector name
func (smc *StorageMetricsCollector) Name() string {
	return smc.name
}

// Collect collects storage metrics
func (smc *StorageMetricsCollector) Collect() ([]Metric, error) {
	if smc.getMetrics == nil {
		return nil, fmt.Errorf("no metrics getter function provided")
	}

	data := smc.getMetrics()
	var metrics []Metric

	for name, value := range data {
		switch v := value.(type) {
		case int64:
			metric := NewGaugeImpl(fmt.Sprintf("storage_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case float64:
			metric := NewGaugeImpl(fmt.Sprintf("storage_%s", name), nil)
			metric.Set(v)
			metrics = append(metrics, metric)
		case int:
			metric := NewGaugeImpl(fmt.Sprintf("storage_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		}
	}

	return metrics, nil
}

// NetworkMetricsCollector collects network-related metrics
type NetworkMetricsCollector struct {
	name       string
	getMetrics func() map[string]interface{}
	mu         sync.RWMutex
}

// NewNetworkMetricsCollector creates a new network metrics collector
func NewNetworkMetricsCollector(getMetrics func() map[string]interface{}) *NetworkMetricsCollector {
	return &NetworkMetricsCollector{
		name:       "network",
		getMetrics: getMetrics,
	}
}

// Name returns the collector name
func (nmc *NetworkMetricsCollector) Name() string {
	return nmc.name
}

// Collect collects network metrics
func (nmc *NetworkMetricsCollector) Collect() ([]Metric, error) {
	if nmc.getMetrics == nil {
		return nil, fmt.Errorf("no metrics getter function provided")
	}

	data := nmc.getMetrics()
	var metrics []Metric

	for name, value := range data {
		switch v := value.(type) {
		case int64:
			metric := NewGaugeImpl(fmt.Sprintf("network_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case float64:
			metric := NewGaugeImpl(fmt.Sprintf("network_%s", name), nil)
			metric.Set(v)
			metrics = append(metrics, metric)
		case int:
			metric := NewGaugeImpl(fmt.Sprintf("network_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		}
	}

	return metrics, nil
}

// RaftMetricsCollector collects Raft consensus metrics
type RaftMetricsCollector struct {
	name       string
	getMetrics func() map[string]interface{}
	mu         sync.RWMutex
}

// NewRaftMetricsCollector creates a new Raft metrics collector
func NewRaftMetricsCollector(getMetrics func() map[string]interface{}) *RaftMetricsCollector {
	return &RaftMetricsCollector{
		name:       "raft",
		getMetrics: getMetrics,
	}
}

// Name returns the collector name
func (rmc *RaftMetricsCollector) Name() string {
	return rmc.name
}

// Collect collects Raft metrics
func (rmc *RaftMetricsCollector) Collect() ([]Metric, error) {
	if rmc.getMetrics == nil {
		return nil, fmt.Errorf("no metrics getter function provided")
	}

	data := rmc.getMetrics()
	var metrics []Metric

	for name, value := range data {
		switch v := value.(type) {
		case int64:
			metric := NewGaugeImpl(fmt.Sprintf("raft_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case float64:
			metric := NewGaugeImpl(fmt.Sprintf("raft_%s", name), nil)
			metric.Set(v)
			metrics = append(metrics, metric)
		case int:
			metric := NewGaugeImpl(fmt.Sprintf("raft_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case time.Duration:
			metric := NewGaugeImpl(fmt.Sprintf("raft_%s_seconds", name), nil)
			metric.Set(v.Seconds())
			metrics = append(metrics, metric)
		case string:
			// For state labels, create a gauge that indicates current state
			labels := map[string]string{"state": v}
			metric := NewGaugeImpl(fmt.Sprintf("raft_%s_info", name), labels)
			metric.Set(1.0)
			metrics = append(metrics, metric)
		}
	}

	return metrics, nil
}

// StreamMetricsCollector collects stream processing metrics
type StreamMetricsCollector struct {
	name       string
	getMetrics func() map[string]interface{}
	mu         sync.RWMutex
}

// NewStreamMetricsCollector creates a new stream metrics collector
func NewStreamMetricsCollector(getMetrics func() map[string]interface{}) *StreamMetricsCollector {
	return &StreamMetricsCollector{
		name:       "stream",
		getMetrics: getMetrics,
	}
}

// Name returns the collector name
func (smc *StreamMetricsCollector) Name() string {
	return smc.name
}

// Collect collects stream processing metrics
func (smc *StreamMetricsCollector) Collect() ([]Metric, error) {
	if smc.getMetrics == nil {
		return nil, fmt.Errorf("no metrics getter function provided")
	}

	data := smc.getMetrics()
	var metrics []Metric

	for name, value := range data {
		switch v := value.(type) {
		case int64:
			metric := NewGaugeImpl(fmt.Sprintf("stream_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case float64:
			metric := NewGaugeImpl(fmt.Sprintf("stream_%s", name), nil)
			metric.Set(v)
			metrics = append(metrics, metric)
		case int:
			metric := NewGaugeImpl(fmt.Sprintf("stream_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case time.Duration:
			metric := NewGaugeImpl(fmt.Sprintf("stream_%s_seconds", name), nil)
			metric.Set(v.Seconds())
			metrics = append(metrics, metric)
		}
	}

	return metrics, nil
}

// ConsumerGroupMetricsCollector collects consumer group metrics
type ConsumerGroupMetricsCollector struct {
	name       string
	getMetrics func() map[string]interface{}
	mu         sync.RWMutex
}

// NewConsumerGroupMetricsCollector creates a new consumer group metrics collector
func NewConsumerGroupMetricsCollector(getMetrics func() map[string]interface{}) *ConsumerGroupMetricsCollector {
	return &ConsumerGroupMetricsCollector{
		name:       "consumer_group",
		getMetrics: getMetrics,
	}
}

// Name returns the collector name
func (cgmc *ConsumerGroupMetricsCollector) Name() string {
	return cgmc.name
}

// Collect collects consumer group metrics
func (cgmc *ConsumerGroupMetricsCollector) Collect() ([]Metric, error) {
	if cgmc.getMetrics == nil {
		return nil, fmt.Errorf("no metrics getter function provided")
	}

	data := cgmc.getMetrics()
	var metrics []Metric

	for name, value := range data {
		switch v := value.(type) {
		case int64:
			metric := NewGaugeImpl(fmt.Sprintf("consumer_group_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case float64:
			metric := NewGaugeImpl(fmt.Sprintf("consumer_group_%s", name), nil)
			metric.Set(v)
			metrics = append(metrics, metric)
		case int:
			metric := NewGaugeImpl(fmt.Sprintf("consumer_group_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case map[string]interface{}:
			// Handle nested metrics (e.g., per-partition metrics)
			for subName, subValue := range v {
				switch sv := subValue.(type) {
				case int64:
					labels := map[string]string{"partition": subName}
					metric := NewGaugeImpl(fmt.Sprintf("consumer_group_%s", name), labels)
					metric.Set(float64(sv))
					metrics = append(metrics, metric)
				case float64:
					labels := map[string]string{"partition": subName}
					metric := NewGaugeImpl(fmt.Sprintf("consumer_group_%s", name), labels)
					metric.Set(sv)
					metrics = append(metrics, metric)
				}
			}
		}
	}

	return metrics, nil
}

// ProducerMetricsCollector collects producer metrics
type ProducerMetricsCollector struct {
	name       string
	getMetrics func() map[string]interface{}
	mu         sync.RWMutex
}

// NewProducerMetricsCollector creates a new producer metrics collector
func NewProducerMetricsCollector(getMetrics func() map[string]interface{}) *ProducerMetricsCollector {
	return &ProducerMetricsCollector{
		name:       "producer",
		getMetrics: getMetrics,
	}
}

// Name returns the collector name
func (pmc *ProducerMetricsCollector) Name() string {
	return pmc.name
}

// Collect collects producer metrics
func (pmc *ProducerMetricsCollector) Collect() ([]Metric, error) {
	if pmc.getMetrics == nil {
		return nil, fmt.Errorf("no metrics getter function provided")
	}

	data := pmc.getMetrics()
	var metrics []Metric

	for name, value := range data {
		switch v := value.(type) {
		case int64:
			metric := NewGaugeImpl(fmt.Sprintf("producer_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case float64:
			metric := NewGaugeImpl(fmt.Sprintf("producer_%s", name), nil)
			metric.Set(v)
			metrics = append(metrics, metric)
		case int:
			metric := NewGaugeImpl(fmt.Sprintf("producer_%s", name), nil)
			metric.Set(float64(v))
			metrics = append(metrics, metric)
		case time.Duration:
			metric := NewGaugeImpl(fmt.Sprintf("producer_%s_seconds", name), nil)
			metric.Set(v.Seconds())
			metrics = append(metrics, metric)
		}
	}

	return metrics, nil
}

// CustomMetricsCollector allows for custom metric collection
type CustomMetricsCollector struct {
	name      string
	collector func() ([]Metric, error)
	mu        sync.RWMutex
}

// NewCustomMetricsCollector creates a new custom metrics collector
func NewCustomMetricsCollector(name string, collector func() ([]Metric, error)) *CustomMetricsCollector {
	return &CustomMetricsCollector{
		name:      name,
		collector: collector,
	}
}

// Name returns the collector name
func (cmc *CustomMetricsCollector) Name() string {
	return cmc.name
}

// Collect collects custom metrics
func (cmc *CustomMetricsCollector) Collect() ([]Metric, error) {
	if cmc.collector == nil {
		return nil, fmt.Errorf("no collector function provided")
	}

	return cmc.collector()
}

// PerformanceMetricsCollector collects performance-related metrics
type PerformanceMetricsCollector struct {
	name               string
	lastCollectionTime time.Time
	lastMetrics        map[string]int64
	mu                 sync.RWMutex
}

// NewPerformanceMetricsCollector creates a new performance metrics collector
func NewPerformanceMetricsCollector() *PerformanceMetricsCollector {
	return &PerformanceMetricsCollector{
		name:               "performance",
		lastCollectionTime: time.Now(),
		lastMetrics:        make(map[string]int64),
	}
}

// Name returns the collector name
func (pmc *PerformanceMetricsCollector) Name() string {
	return pmc.name
}

// Collect collects performance metrics
func (pmc *PerformanceMetricsCollector) Collect() ([]Metric, error) {
	pmc.mu.Lock()
	defer pmc.mu.Unlock()

	now := time.Now()
	timeDelta := now.Sub(pmc.lastCollectionTime).Seconds()
	pmc.lastCollectionTime = now

	var metrics []Metric

	// Get current memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate rates
	currentAlloc := int64(memStats.TotalAlloc)
	if lastAlloc, exists := pmc.lastMetrics["total_alloc"]; exists && timeDelta > 0 {
		allocRate := float64(currentAlloc-lastAlloc) / timeDelta
		metric := NewGaugeImpl("performance_memory_alloc_rate_bytes_per_second", nil)
		metric.Set(allocRate)
		metrics = append(metrics, metric)
	}
	pmc.lastMetrics["total_alloc"] = currentAlloc

	// GC rate
	currentGCRuns := int64(memStats.NumGC)
	if lastGCRuns, exists := pmc.lastMetrics["gc_runs"]; exists && timeDelta > 0 {
		gcRate := float64(currentGCRuns-lastGCRuns) / timeDelta
		metric := NewGaugeImpl("performance_gc_rate_per_second", nil)
		metric.Set(gcRate)
		metrics = append(metrics, metric)
	}
	pmc.lastMetrics["gc_runs"] = currentGCRuns

	// CPU usage (simplified - goroutine scheduler stats)
	metric := NewGaugeImpl("performance_cpu_utilization", nil)
	metric.Set(float64(runtime.NumGoroutine()) / float64(runtime.GOMAXPROCS(0)))
	metrics = append(metrics, metric)

	return metrics, nil
}
