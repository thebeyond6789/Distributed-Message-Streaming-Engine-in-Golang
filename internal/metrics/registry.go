package metrics

import (
	"fmt"
	"sync"
	"time"
)

// MetricRegistryImpl implements MetricRegistry interface
type MetricRegistryImpl struct {
	metrics map[string]Metric
	mu      sync.RWMutex
	config  *MetricsConfig
}

// NewMetricRegistry creates a new metric registry
func NewMetricRegistry(config *MetricsConfig) *MetricRegistryImpl {
	if config == nil {
		config = DefaultMetricsConfig()
	}

	return &MetricRegistryImpl{
		metrics: make(map[string]Metric),
		config:  config,
	}
}

// Counter returns a counter metric
func (r *MetricRegistryImpl) Counter(name string, labels map[string]string) *CounterImpl {
	key := r.metricKey(name, labels)

	r.mu.RLock()
	if metric, exists := r.metrics[key]; exists {
		r.mu.RUnlock()
		if counter, ok := metric.(*CounterImpl); ok {
			return counter
		}
		// Metric exists but wrong type - this shouldn't happen in normal usage
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if metric, exists := r.metrics[key]; exists {
		if counter, ok := metric.(*CounterImpl); ok {
			return counter
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}

	// Add global labels
	mergedLabels := r.mergeLabels(labels)
	counter := NewCounterImpl(name, mergedLabels)
	r.metrics[key] = counter

	return counter
}

// Gauge returns a gauge metric
func (r *MetricRegistryImpl) Gauge(name string, labels map[string]string) *GaugeImpl {
	key := r.metricKey(name, labels)

	r.mu.RLock()
	if metric, exists := r.metrics[key]; exists {
		r.mu.RUnlock()
		if gauge, ok := metric.(*GaugeImpl); ok {
			return gauge
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if metric, exists := r.metrics[key]; exists {
		if gauge, ok := metric.(*GaugeImpl); ok {
			return gauge
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}

	// Add global labels
	mergedLabels := r.mergeLabels(labels)
	gauge := NewGaugeImpl(name, mergedLabels)
	r.metrics[key] = gauge

	return gauge
}

// Histogram returns a histogram metric
func (r *MetricRegistryImpl) Histogram(name string, labels map[string]string, buckets []float64) *HistogramImpl {
	key := r.metricKey(name, labels)

	r.mu.RLock()
	if metric, exists := r.metrics[key]; exists {
		r.mu.RUnlock()
		if histogram, ok := metric.(*HistogramImpl); ok {
			return histogram
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if metric, exists := r.metrics[key]; exists {
		if histogram, ok := metric.(*HistogramImpl); ok {
			return histogram
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}

	// Use default buckets if none provided
	if buckets == nil {
		buckets = r.config.DefaultHistogramBuckets
	}

	// Add global labels
	mergedLabels := r.mergeLabels(labels)
	histogram := NewHistogramImpl(name, mergedLabels, buckets)
	r.metrics[key] = histogram

	return histogram
}

// Timer returns a timer metric
func (r *MetricRegistryImpl) Timer(name string, labels map[string]string) *TimerImpl {
	key := r.metricKey(name, labels)

	r.mu.RLock()
	if metric, exists := r.metrics[key]; exists {
		r.mu.RUnlock()
		if timer, ok := metric.(*TimerImpl); ok {
			return timer
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if metric, exists := r.metrics[key]; exists {
		if timer, ok := metric.(*TimerImpl); ok {
			return timer
		}
		panic(fmt.Sprintf("metric %s already exists with different type", key))
	}

	// Add global labels
	mergedLabels := r.mergeLabels(labels)
	timer := NewTimerImpl(name, mergedLabels)
	r.metrics[key] = timer

	return timer
}

// GetMetric returns a specific metric
func (r *MetricRegistryImpl) GetMetric(name string, labels map[string]string) Metric {
	key := r.metricKey(name, labels)

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.metrics[key]
}

// GetAllMetrics returns all registered metrics
func (r *MetricRegistryImpl) GetAllMetrics() []Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()

	metrics := make([]Metric, 0, len(r.metrics))
	for _, metric := range r.metrics {
		metrics = append(metrics, metric.Clone())
	}

	return metrics
}

// UnregisterMetric removes a metric from the registry
func (r *MetricRegistryImpl) UnregisterMetric(name string, labels map[string]string) {
	key := r.metricKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.metrics, key)
}

// Clear removes all metrics from the registry
func (r *MetricRegistryImpl) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics = make(map[string]Metric)
}

// GetMetricCount returns the number of registered metrics
func (r *MetricRegistryImpl) GetMetricCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.metrics)
}

// Helper methods

func (r *MetricRegistryImpl) metricKey(name string, labels map[string]string) string {
	key := name
	if len(labels) > 0 {
		key += "{"
		first := true
		for k, v := range labels {
			if !first {
				key += ","
			}
			key += fmt.Sprintf("%s=%s", k, v)
			first = false
		}
		key += "}"
	}
	return key
}

func (r *MetricRegistryImpl) mergeLabels(labels map[string]string) map[string]string {
	merged := make(map[string]string)

	// Add global labels first
	for k, v := range r.config.GlobalLabels {
		merged[k] = v
	}

	// Add specific labels (these override global labels)
	for k, v := range labels {
		merged[k] = v
	}

	return merged
}

// MetricsManager manages the collection and export of metrics
type MetricsManager struct {
	registry   MetricRegistry
	collectors []MetricCollector
	exporters  []MetricExporter
	config     *MetricsConfig

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// NewMetricsManager creates a new metrics manager
func NewMetricsManager(config *MetricsConfig) *MetricsManager {
	if config == nil {
		config = DefaultMetricsConfig()
	}

	return &MetricsManager{
		registry:   NewMetricRegistry(config),
		collectors: make([]MetricCollector, 0),
		exporters:  make([]MetricExporter, 0),
		config:     config,
		stopCh:     make(chan struct{}),
	}
}

// GetRegistry returns the metric registry
func (mm *MetricsManager) GetRegistry() MetricRegistry {
	return mm.registry
}

// AddCollector adds a metric collector
func (mm *MetricsManager) AddCollector(collector MetricCollector) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.collectors = append(mm.collectors, collector)
}

// RemoveCollector removes a metric collector
func (mm *MetricsManager) RemoveCollector(name string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	for i, collector := range mm.collectors {
		if collector.Name() == name {
			mm.collectors = append(mm.collectors[:i], mm.collectors[i+1:]...)
			break
		}
	}
}

// AddExporter adds a metric exporter
func (mm *MetricsManager) AddExporter(exporter MetricExporter) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.exporters = append(mm.exporters, exporter)
}

// RemoveExporter removes a metric exporter
func (mm *MetricsManager) RemoveExporter(exporter MetricExporter) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	for i, exp := range mm.exporters {
		if exp == exporter {
			mm.exporters = append(mm.exporters[:i], mm.exporters[i+1:]...)
			break
		}
	}
}

// Start starts the metrics collection and export
func (mm *MetricsManager) Start() error {
	if !mm.config.Enabled {
		return nil
	}

	mm.wg.Add(1)
	go mm.collectionLoop()

	return nil
}

// Stop stops the metrics collection and export
func (mm *MetricsManager) Stop() error {
	close(mm.stopCh)
	mm.wg.Wait()

	// Close all exporters
	mm.mu.Lock()
	defer mm.mu.Unlock()

	for _, exporter := range mm.exporters {
		exporter.Close()
	}

	return nil
}

// CollectMetrics collects metrics from all collectors
func (mm *MetricsManager) CollectMetrics() ([]Metric, error) {
	mm.mu.RLock()
	collectors := make([]MetricCollector, len(mm.collectors))
	copy(collectors, mm.collectors)
	mm.mu.RUnlock()

	var allMetrics []Metric

	// Collect from registry
	registryMetrics := mm.registry.GetAllMetrics()
	allMetrics = append(allMetrics, registryMetrics...)

	// Collect from collectors
	for _, collector := range collectors {
		metrics, err := collector.Collect()
		if err != nil {
			// Log error but continue with other collectors
			continue
		}
		allMetrics = append(allMetrics, metrics...)
	}

	return allMetrics, nil
}

// ExportMetrics exports metrics using all exporters
func (mm *MetricsManager) ExportMetrics(metrics []Metric) error {
	mm.mu.RLock()
	exporters := make([]MetricExporter, len(mm.exporters))
	copy(exporters, mm.exporters)
	mm.mu.RUnlock()

	var lastErr error

	for _, exporter := range exporters {
		if err := exporter.Export(metrics); err != nil {
			lastErr = err
			// Continue with other exporters
		}
	}

	return lastErr
}

// GetMetrics returns current metrics snapshot
func (mm *MetricsManager) GetMetrics() ([]Metric, error) {
	return mm.CollectMetrics()
}

func (mm *MetricsManager) collectionLoop() {
	defer mm.wg.Done()

	ticker := time.NewTicker(mm.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.stopCh:
			return
		case <-ticker.C:
			metrics, err := mm.CollectMetrics()
			if err != nil {
				continue
			}

			if err := mm.ExportMetrics(metrics); err != nil {
				// Log error but continue
				continue
			}
		}
	}
}

// GlobalRegistry is a global instance of MetricRegistry
var GlobalRegistry MetricRegistry = NewMetricRegistry(DefaultMetricsConfig())

// Global convenience functions

// NewCounter returns a counter from the global registry
func NewCounter(name string, labels map[string]string) *CounterImpl {
	return GlobalRegistry.Counter(name, labels)
}

// NewGauge returns a gauge from the global registry
func NewGauge(name string, labels map[string]string) *GaugeImpl {
	return GlobalRegistry.Gauge(name, labels)
}

// NewHistogram returns a histogram from the global registry
func NewHistogram(name string, labels map[string]string, buckets []float64) *HistogramImpl {
	return GlobalRegistry.Histogram(name, labels, buckets)
}

// NewTimer returns a timer from the global registry
func NewTimer(name string, labels map[string]string) *TimerImpl {
	return GlobalRegistry.Timer(name, labels)
}

// SetGlobalRegistry sets the global registry
func SetGlobalRegistry(registry MetricRegistry) {
	GlobalRegistry = registry
}
