package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MonitoringService provides comprehensive monitoring capabilities
type MonitoringService struct {
	metricsManager *MetricsManager
	alertManager   *AlertManager
	healthChecker  *HealthChecker
	config         *MonitoringConfig

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// MonitoringConfig represents monitoring configuration
type MonitoringConfig struct {
	MetricsConfig     *MetricsConfig     `json:"metrics_config"`
	AlertConfig       *AlertConfig       `json:"alert_config"`
	HealthCheckConfig *HealthCheckConfig `json:"health_check_config"`

	// Monitoring intervals
	MetricsInterval     time.Duration `json:"metrics_interval"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	AlertCheckInterval  time.Duration `json:"alert_check_interval"`

	// Enable/disable features
	EnableMetrics     bool `json:"enable_metrics"`
	EnableAlerts      bool `json:"enable_alerts"`
	EnableHealthCheck bool `json:"enable_health_check"`
}

// DefaultMonitoringConfig returns default monitoring configuration
func DefaultMonitoringConfig() *MonitoringConfig {
	return &MonitoringConfig{
		MetricsConfig:       DefaultMetricsConfig(),
		AlertConfig:         DefaultAlertConfig(),
		HealthCheckConfig:   DefaultHealthCheckConfig(),
		MetricsInterval:     10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
		AlertCheckInterval:  5 * time.Second,
		EnableMetrics:       true,
		EnableAlerts:        true,
		EnableHealthCheck:   true,
	}
}

// NewMonitoringService creates a new monitoring service
func NewMonitoringService(config *MonitoringConfig) *MonitoringService {
	if config == nil {
		config = DefaultMonitoringConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	ms := &MonitoringService{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize components
	if config.EnableMetrics {
		ms.metricsManager = NewMetricsManager(config.MetricsConfig)
	}

	if config.EnableAlerts {
		ms.alertManager = NewAlertManager(config.AlertConfig)
	}

	if config.EnableHealthCheck {
		ms.healthChecker = NewHealthChecker(config.HealthCheckConfig)
	}

	return ms
}

// Start starts the monitoring service
func (ms *MonitoringService) Start() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Start metrics manager
	if ms.metricsManager != nil {
		if err := ms.metricsManager.Start(); err != nil {
			return fmt.Errorf("failed to start metrics manager: %w", err)
		}
	}

	// Start alert manager
	if ms.alertManager != nil {
		if err := ms.alertManager.Start(); err != nil {
			return fmt.Errorf("failed to start alert manager: %w", err)
		}
	}

	// Start health checker
	if ms.healthChecker != nil {
		if err := ms.healthChecker.Start(); err != nil {
			return fmt.Errorf("failed to start health checker: %w", err)
		}
	}

	// Start monitoring loops
	ms.wg.Add(1)
	go ms.monitoringLoop()

	return nil
}

// Stop stops the monitoring service
func (ms *MonitoringService) Stop() error {
	ms.cancel()
	ms.wg.Wait()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	var lastErr error

	// Stop components
	if ms.metricsManager != nil {
		if err := ms.metricsManager.Stop(); err != nil {
			lastErr = err
		}
	}

	if ms.alertManager != nil {
		if err := ms.alertManager.Stop(); err != nil {
			lastErr = err
		}
	}

	if ms.healthChecker != nil {
		if err := ms.healthChecker.Stop(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// GetMetricsManager returns the metrics manager
func (ms *MonitoringService) GetMetricsManager() *MetricsManager {
	return ms.metricsManager
}

// GetAlertManager returns the alert manager
func (ms *MonitoringService) GetAlertManager() *AlertManager {
	return ms.alertManager
}

// GetHealthChecker returns the health checker
func (ms *MonitoringService) GetHealthChecker() *HealthChecker {
	return ms.healthChecker
}

// RegisterHealthCheck registers a health check
func (ms *MonitoringService) RegisterHealthCheck(name string, check HealthCheck) {
	if ms.healthChecker != nil {
		ms.healthChecker.RegisterCheck(name, check)
	}
}

// RegisterAlert registers an alert rule
func (ms *MonitoringService) RegisterAlert(rule *AlertRule) {
	if ms.alertManager != nil {
		ms.alertManager.RegisterRule(rule)
	}
}

// AddMetricCollector adds a metric collector
func (ms *MonitoringService) AddMetricCollector(collector MetricCollector) {
	if ms.metricsManager != nil {
		ms.metricsManager.AddCollector(collector)
	}
}

// AddMetricExporter adds a metric exporter
func (ms *MonitoringService) AddMetricExporter(exporter MetricExporter) {
	if ms.metricsManager != nil {
		ms.metricsManager.AddExporter(exporter)
	}
}

// GetStatus returns the overall monitoring status
func (ms *MonitoringService) GetStatus() *MonitoringStatus {
	status := &MonitoringStatus{
		Timestamp: time.Now(),
	}

	// Get metrics status
	if ms.metricsManager != nil {
		metrics, _ := ms.metricsManager.GetMetrics()
		status.MetricsCount = len(metrics)
		status.MetricsEnabled = true
	}

	// Get health status
	if ms.healthChecker != nil {
		healthStatus := ms.healthChecker.GetOverallStatus()
		status.HealthStatus = healthStatus.Status
		status.HealthCheckCount = len(healthStatus.Checks)
		status.HealthEnabled = true
	}

	// Get alert status
	if ms.alertManager != nil {
		alertStatus := ms.alertManager.GetStatus()
		status.ActiveAlerts = alertStatus.ActiveAlerts
		status.AlertRuleCount = alertStatus.RuleCount
		status.AlertsEnabled = true
	}

	return status
}

func (ms *MonitoringService) monitoringLoop() {
	defer ms.wg.Done()

	metricsTicker := time.NewTicker(ms.config.MetricsInterval)
	healthTicker := time.NewTicker(ms.config.HealthCheckInterval)
	alertTicker := time.NewTicker(ms.config.AlertCheckInterval)

	defer metricsTicker.Stop()
	defer healthTicker.Stop()
	defer alertTicker.Stop()

	for {
		select {
		case <-ms.ctx.Done():
			return

		case <-metricsTicker.C:
			ms.processMetrics()

		case <-healthTicker.C:
			ms.processHealthChecks()

		case <-alertTicker.C:
			ms.processAlerts()
		}
	}
}

func (ms *MonitoringService) processMetrics() {
	if ms.metricsManager == nil {
		return
	}

	// Metrics are automatically collected by the metrics manager
	// This could be used for additional processing if needed
}

func (ms *MonitoringService) processHealthChecks() {
	if ms.healthChecker == nil {
		return
	}

	// Health checks are automatically executed by the health checker
	// This could be used for additional processing if needed
}

func (ms *MonitoringService) processAlerts() {
	if ms.alertManager == nil || ms.metricsManager == nil {
		return
	}

	// Get current metrics for alert evaluation
	metrics, err := ms.metricsManager.GetMetrics()
	if err != nil {
		return
	}

	// Evaluate alerts
	ms.alertManager.EvaluateAlerts(metrics)
}

// MonitoringStatus represents the current monitoring status
type MonitoringStatus struct {
	Timestamp time.Time `json:"timestamp"`

	// Metrics
	MetricsEnabled bool `json:"metrics_enabled"`
	MetricsCount   int  `json:"metrics_count"`

	// Health checks
	HealthEnabled    bool         `json:"health_enabled"`
	HealthStatus     HealthStatus `json:"health_status"`
	HealthCheckCount int          `json:"health_check_count"`

	// Alerts
	AlertsEnabled  bool `json:"alerts_enabled"`
	ActiveAlerts   int  `json:"active_alerts"`
	AlertRuleCount int  `json:"alert_rule_count"`
}

// PerformanceMonitor monitors system performance
type PerformanceMonitor struct {
	registry MetricRegistry

	// CPU metrics
	cpuUsageGauge       *GaugeImpl
	goroutineCountGauge *GaugeImpl

	// Memory metrics
	memoryUsageGauge *GaugeImpl
	memoryAllocGauge *GaugeImpl
	heapUsageGauge   *GaugeImpl
	gcCountCounter   *CounterImpl
	gcPauseHistogram *HistogramImpl

	// Network metrics
	connectionCountGauge *GaugeImpl
	bytesReceivedCounter *CounterImpl
	bytesSentCounter     *CounterImpl

	// Application metrics
	requestRateGauge *GaugeImpl
	responseTimeHist *HistogramImpl
	errorRateGauge   *GaugeImpl

	mu sync.RWMutex
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(registry MetricRegistry) *PerformanceMonitor {
	pm := &PerformanceMonitor{
		registry: registry,
	}

	// Initialize metrics
	pm.cpuUsageGauge = registry.Gauge("cpu_usage_percent", nil)
	pm.goroutineCountGauge = registry.Gauge("goroutine_count", nil)
	pm.memoryUsageGauge = registry.Gauge("memory_usage_bytes", nil)
	pm.memoryAllocGauge = registry.Gauge("memory_alloc_bytes", nil)
	pm.heapUsageGauge = registry.Gauge("heap_usage_bytes", nil)
	pm.gcCountCounter = registry.Counter("gc_count_total", nil)
	pm.gcPauseHistogram = registry.Histogram("gc_pause_seconds", nil, nil)
	pm.connectionCountGauge = registry.Gauge("connection_count", nil)
	pm.bytesReceivedCounter = registry.Counter("bytes_received_total", nil)
	pm.bytesSentCounter = registry.Counter("bytes_sent_total", nil)
	pm.requestRateGauge = registry.Gauge("request_rate_per_second", nil)
	pm.responseTimeHist = registry.Histogram("response_time_seconds", nil, nil)
	pm.errorRateGauge = registry.Gauge("error_rate_percent", nil)

	return pm
}

// UpdateCPUUsage updates CPU usage metrics
func (pm *PerformanceMonitor) UpdateCPUUsage(percent float64) {
	pm.cpuUsageGauge.Set(percent)
}

// UpdateGoroutineCount updates goroutine count
func (pm *PerformanceMonitor) UpdateGoroutineCount(count int) {
	pm.goroutineCountGauge.Set(float64(count))
}

// UpdateMemoryUsage updates memory usage metrics
func (pm *PerformanceMonitor) UpdateMemoryUsage(usage, alloc, heap int64) {
	pm.memoryUsageGauge.Set(float64(usage))
	pm.memoryAllocGauge.Set(float64(alloc))
	pm.heapUsageGauge.Set(float64(heap))
}

// UpdateGCMetrics updates garbage collection metrics
func (pm *PerformanceMonitor) UpdateGCMetrics(count int64, pauseNS int64) {
	pm.gcCountCounter.Add(count)
	pm.gcPauseHistogram.Observe(float64(pauseNS) / 1e9) // Convert to seconds
}

// UpdateNetworkMetrics updates network metrics
func (pm *PerformanceMonitor) UpdateNetworkMetrics(connections int, bytesReceived, bytesSent int64) {
	pm.connectionCountGauge.Set(float64(connections))
	pm.bytesReceivedCounter.Add(bytesReceived)
	pm.bytesSentCounter.Add(bytesSent)
}

// UpdateApplicationMetrics updates application-level metrics
func (pm *PerformanceMonitor) UpdateApplicationMetrics(requestRate, errorRate float64, responseTime time.Duration) {
	pm.requestRateGauge.Set(requestRate)
	pm.errorRateGauge.Set(errorRate)
	pm.responseTimeHist.Observe(responseTime.Seconds())
}

// ApplicationMonitor monitors application-specific metrics
type ApplicationMonitor struct {
	registry MetricRegistry

	// Message metrics
	messagesProducedCounter *CounterImpl
	messagesConsumedCounter *CounterImpl
	messagesSizeHistogram   *HistogramImpl

	// Topic metrics
	topicCountGauge     *GaugeImpl
	partitionCountGauge *GaugeImpl

	// Consumer group metrics
	consumerGroupCountGauge *GaugeImpl
	lagHistogram            *HistogramImpl

	// Replication metrics
	replicationFactorGauge *GaugeImpl
	syncReplicasGauge      *GaugeImpl

	mu sync.RWMutex
}

// NewApplicationMonitor creates a new application monitor
func NewApplicationMonitor(registry MetricRegistry) *ApplicationMonitor {
	am := &ApplicationMonitor{
		registry: registry,
	}

	// Initialize metrics
	am.messagesProducedCounter = registry.Counter("messages_produced_total", nil)
	am.messagesConsumedCounter = registry.Counter("messages_consumed_total", nil)
	am.messagesSizeHistogram = registry.Histogram("message_size_bytes", nil, []float64{
		100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024, 10 * 1024 * 1024,
	})
	am.topicCountGauge = registry.Gauge("topic_count", nil)
	am.partitionCountGauge = registry.Gauge("partition_count", nil)
	am.consumerGroupCountGauge = registry.Gauge("consumer_group_count", nil)
	am.lagHistogram = registry.Histogram("consumer_lag_seconds", nil, nil)
	am.replicationFactorGauge = registry.Gauge("replication_factor", nil)
	am.syncReplicasGauge = registry.Gauge("sync_replicas", nil)

	return am
}

// RecordMessageProduced records a produced message
func (am *ApplicationMonitor) RecordMessageProduced(topic string, size int) {
	labels := map[string]string{"topic": topic}
	counter := am.registry.Counter("messages_produced_total", labels)
	counter.Inc()

	am.messagesSizeHistogram.Observe(float64(size))
}

// RecordMessageConsumed records a consumed message
func (am *ApplicationMonitor) RecordMessageConsumed(topic string, lag time.Duration) {
	labels := map[string]string{"topic": topic}
	counter := am.registry.Counter("messages_consumed_total", labels)
	counter.Inc()

	am.lagHistogram.Observe(lag.Seconds())
}

// UpdateTopicMetrics updates topic-related metrics
func (am *ApplicationMonitor) UpdateTopicMetrics(topicCount, partitionCount int) {
	am.topicCountGauge.Set(float64(topicCount))
	am.partitionCountGauge.Set(float64(partitionCount))
}

// UpdateConsumerGroupMetrics updates consumer group metrics
func (am *ApplicationMonitor) UpdateConsumerGroupMetrics(groupCount int) {
	am.consumerGroupCountGauge.Set(float64(groupCount))
}

// UpdateReplicationMetrics updates replication metrics
func (am *ApplicationMonitor) UpdateReplicationMetrics(replicationFactor, syncReplicas int) {
	am.replicationFactorGauge.Set(float64(replicationFactor))
	am.syncReplicasGauge.Set(float64(syncReplicas))
}
