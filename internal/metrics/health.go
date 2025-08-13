package metrics

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// HealthStatus represents the health status
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// HealthCheck represents a health check interface
type HealthCheck interface {
	Name() string
	Check(ctx context.Context) HealthCheckResult
	Timeout() time.Duration
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Status    HealthStatus           `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Duration  time.Duration          `json:"duration"`
	Error     error                  `json:"error,omitempty"`
}

// HealthCheckConfig represents health check configuration
type HealthCheckConfig struct {
	Enabled       bool          `json:"enabled"`
	CheckInterval time.Duration `json:"check_interval"`
	Timeout       time.Duration `json:"timeout"`
	MaxFailures   int           `json:"max_failures"`
	FailureWindow time.Duration `json:"failure_window"`
	StartupGrace  time.Duration `json:"startup_grace"`
}

// DefaultHealthCheckConfig returns default health check configuration
func DefaultHealthCheckConfig() *HealthCheckConfig {
	return &HealthCheckConfig{
		Enabled:       true,
		CheckInterval: 30 * time.Second,
		Timeout:       5 * time.Second,
		MaxFailures:   3,
		FailureWindow: 5 * time.Minute,
		StartupGrace:  30 * time.Second,
	}
}

// HealthChecker manages health checks
type HealthChecker struct {
	config  *HealthCheckConfig
	checks  map[string]HealthCheck
	results map[string]*HealthCheckHistory

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex

	startTime time.Time
}

// HealthCheckHistory tracks the history of health check results
type HealthCheckHistory struct {
	CurrentResult HealthCheckResult   `json:"current_result"`
	Results       []HealthCheckResult `json:"results"`
	FailureCount  int                 `json:"failure_count"`
	LastSuccess   time.Time           `json:"last_success"`
	LastFailure   time.Time           `json:"last_failure"`
	mu            sync.RWMutex
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(config *HealthCheckConfig) *HealthChecker {
	if config == nil {
		config = DefaultHealthCheckConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &HealthChecker{
		config:    config,
		checks:    make(map[string]HealthCheck),
		results:   make(map[string]*HealthCheckHistory),
		ctx:       ctx,
		cancel:    cancel,
		startTime: time.Now(),
	}
}

// Start starts the health checker
func (hc *HealthChecker) Start() error {
	if !hc.config.Enabled {
		return nil
	}

	hc.wg.Add(1)
	go hc.checkLoop()

	return nil
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() error {
	hc.cancel()
	hc.wg.Wait()
	return nil
}

// RegisterCheck registers a health check
func (hc *HealthChecker) RegisterCheck(name string, check HealthCheck) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.checks[name] = check
	hc.results[name] = &HealthCheckHistory{
		Results: make([]HealthCheckResult, 0),
	}
}

// UnregisterCheck unregisters a health check
func (hc *HealthChecker) UnregisterCheck(name string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	delete(hc.checks, name)
	delete(hc.results, name)
}

// RunCheck runs a specific health check
func (hc *HealthChecker) RunCheck(name string) (HealthCheckResult, error) {
	hc.mu.RLock()
	check, exists := hc.checks[name]
	hc.mu.RUnlock()

	if !exists {
		return HealthCheckResult{}, fmt.Errorf("health check not found: %s", name)
	}

	return hc.executeCheck(check), nil
}

// RunAllChecks runs all registered health checks
func (hc *HealthChecker) RunAllChecks() map[string]HealthCheckResult {
	hc.mu.RLock()
	checks := make(map[string]HealthCheck)
	for name, check := range hc.checks {
		checks[name] = check
	}
	hc.mu.RUnlock()

	results := make(map[string]HealthCheckResult)

	// Run checks in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex

	for name, check := range checks {
		wg.Add(1)
		go func(n string, c HealthCheck) {
			defer wg.Done()
			result := hc.executeCheck(c)

			mu.Lock()
			results[n] = result
			mu.Unlock()
		}(name, check)
	}

	wg.Wait()
	return results
}

// GetCheckResult returns the latest result for a specific check
func (hc *HealthChecker) GetCheckResult(name string) (HealthCheckResult, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	history, exists := hc.results[name]
	if !exists {
		return HealthCheckResult{}, fmt.Errorf("health check not found: %s", name)
	}

	history.mu.RLock()
	defer history.mu.RUnlock()

	return history.CurrentResult, nil
}

// GetAllResults returns results for all health checks
func (hc *HealthChecker) GetAllResults() map[string]HealthCheckResult {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	results := make(map[string]HealthCheckResult)

	for name, history := range hc.results {
		history.mu.RLock()
		results[name] = history.CurrentResult
		history.mu.RUnlock()
	}

	return results
}

// GetOverallStatus returns the overall health status
func (hc *HealthChecker) GetOverallStatus() *OverallHealthStatus {
	results := hc.GetAllResults()

	status := &OverallHealthStatus{
		Status:    HealthStatusHealthy,
		Timestamp: time.Now(),
		Checks:    results,
		Uptime:    time.Since(hc.startTime),
	}

	unhealthyCount := 0
	degradedCount := 0

	for _, result := range results {
		switch result.Status {
		case HealthStatusUnhealthy:
			unhealthyCount++
		case HealthStatusDegraded:
			degradedCount++
		}
	}

	// Determine overall status
	if unhealthyCount > 0 {
		status.Status = HealthStatusUnhealthy
	} else if degradedCount > 0 {
		status.Status = HealthStatusDegraded
	}

	return status
}

// IsHealthy returns true if the overall status is healthy
func (hc *HealthChecker) IsHealthy() bool {
	return hc.GetOverallStatus().Status == HealthStatusHealthy
}

func (hc *HealthChecker) checkLoop() {
	defer hc.wg.Done()

	// Wait for startup grace period
	select {
	case <-hc.ctx.Done():
		return
	case <-time.After(hc.config.StartupGrace):
	}

	ticker := time.NewTicker(hc.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.runAllChecks()
		}
	}
}

func (hc *HealthChecker) runAllChecks() {
	results := hc.RunAllChecks()

	// Update history
	hc.mu.Lock()
	for name, result := range results {
		if history, exists := hc.results[name]; exists {
			hc.updateHistory(history, result)
		}
	}
	hc.mu.Unlock()
}

func (hc *HealthChecker) executeCheck(check HealthCheck) HealthCheckResult {
	start := time.Now()

	// Create timeout context
	timeout := check.Timeout()
	if timeout == 0 {
		timeout = hc.config.Timeout
	}

	ctx, cancel := context.WithTimeout(hc.ctx, timeout)
	defer cancel()

	// Execute check
	result := check.Check(ctx)
	result.Timestamp = start
	result.Duration = time.Since(start)

	return result
}

func (hc *HealthChecker) updateHistory(history *HealthCheckHistory, result HealthCheckResult) {
	history.mu.Lock()
	defer history.mu.Unlock()

	// Update current result
	history.CurrentResult = result

	// Add to history (keep last 100 results)
	history.Results = append(history.Results, result)
	if len(history.Results) > 100 {
		history.Results = history.Results[1:]
	}

	// Update failure tracking
	if result.Status == HealthStatusUnhealthy {
		history.FailureCount++
		history.LastFailure = result.Timestamp
	} else if result.Status == HealthStatusHealthy {
		history.LastSuccess = result.Timestamp

		// Reset failure count if we're outside the failure window
		if time.Since(history.LastFailure) > hc.config.FailureWindow {
			history.FailureCount = 0
		}
	}
}

// OverallHealthStatus represents the overall health status
type OverallHealthStatus struct {
	Status    HealthStatus                 `json:"status"`
	Timestamp time.Time                    `json:"timestamp"`
	Uptime    time.Duration                `json:"uptime"`
	Checks    map[string]HealthCheckResult `json:"checks"`
}

// Common health checks

// PingHealthCheck performs a simple ping check
type PingHealthCheck struct {
	name    string
	timeout time.Duration
}

// NewPingHealthCheck creates a new ping health check
func NewPingHealthCheck(name string, timeout time.Duration) *PingHealthCheck {
	return &PingHealthCheck{
		name:    name,
		timeout: timeout,
	}
}

// Name returns the check name
func (phc *PingHealthCheck) Name() string {
	return phc.name
}

// Check performs the ping check
func (phc *PingHealthCheck) Check(ctx context.Context) HealthCheckResult {
	// Simulate a simple ping check
	select {
	case <-ctx.Done():
		return HealthCheckResult{
			Status:  HealthStatusUnhealthy,
			Message: "Check timed out",
			Error:   ctx.Err(),
		}
	case <-time.After(10 * time.Millisecond):
		return HealthCheckResult{
			Status:  HealthStatusHealthy,
			Message: "Ping successful",
		}
	}
}

// Timeout returns the check timeout
func (phc *PingHealthCheck) Timeout() time.Duration {
	return phc.timeout
}

// DatabaseHealthCheck performs a database connectivity check
type DatabaseHealthCheck struct {
	name    string
	timeout time.Duration
	checker func(ctx context.Context) error
}

// NewDatabaseHealthCheck creates a new database health check
func NewDatabaseHealthCheck(name string, timeout time.Duration, checker func(ctx context.Context) error) *DatabaseHealthCheck {
	return &DatabaseHealthCheck{
		name:    name,
		timeout: timeout,
		checker: checker,
	}
}

// Name returns the check name
func (dhc *DatabaseHealthCheck) Name() string {
	return dhc.name
}

// Check performs the database check
func (dhc *DatabaseHealthCheck) Check(ctx context.Context) HealthCheckResult {
	if dhc.checker == nil {
		return HealthCheckResult{
			Status:  HealthStatusUnknown,
			Message: "No checker function provided",
		}
	}

	if err := dhc.checker(ctx); err != nil {
		return HealthCheckResult{
			Status:  HealthStatusUnhealthy,
			Message: fmt.Sprintf("Database check failed: %v", err),
			Error:   err,
		}
	}

	return HealthCheckResult{
		Status:  HealthStatusHealthy,
		Message: "Database connection successful",
	}
}

// Timeout returns the check timeout
func (dhc *DatabaseHealthCheck) Timeout() time.Duration {
	return dhc.timeout
}

// ServiceHealthCheck performs a service endpoint check
type ServiceHealthCheck struct {
	name     string
	timeout  time.Duration
	endpoint string
	client   *http.Client
}

// NewServiceHealthCheck creates a new service health check
func NewServiceHealthCheck(name, endpoint string, timeout time.Duration) *ServiceHealthCheck {
	return &ServiceHealthCheck{
		name:     name,
		timeout:  timeout,
		endpoint: endpoint,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Name returns the check name
func (shc *ServiceHealthCheck) Name() string {
	return shc.name
}

// Check performs the service check
func (shc *ServiceHealthCheck) Check(ctx context.Context) HealthCheckResult {
	req, err := http.NewRequestWithContext(ctx, "GET", shc.endpoint, nil)
	if err != nil {
		return HealthCheckResult{
			Status:  HealthStatusUnhealthy,
			Message: fmt.Sprintf("Failed to create request: %v", err),
			Error:   err,
		}
	}

	resp, err := shc.client.Do(req)
	if err != nil {
		return HealthCheckResult{
			Status:  HealthStatusUnhealthy,
			Message: fmt.Sprintf("Service check failed: %v", err),
			Error:   err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return HealthCheckResult{
			Status:  HealthStatusHealthy,
			Message: fmt.Sprintf("Service responded with status %d", resp.StatusCode),
			Details: map[string]interface{}{
				"status_code": resp.StatusCode,
				"endpoint":    shc.endpoint,
			},
		}
	}

	return HealthCheckResult{
		Status:  HealthStatusDegraded,
		Message: fmt.Sprintf("Service responded with status %d", resp.StatusCode),
		Details: map[string]interface{}{
			"status_code": resp.StatusCode,
			"endpoint":    shc.endpoint,
		},
	}
}

// Timeout returns the check timeout
func (shc *ServiceHealthCheck) Timeout() time.Duration {
	return shc.timeout
}

// MemoryHealthCheck checks memory usage
type MemoryHealthCheck struct {
	name             string
	timeout          time.Duration
	maxMemoryPercent float64
	maxHeapPercent   float64
}

// NewMemoryHealthCheck creates a new memory health check
func NewMemoryHealthCheck(name string, timeout time.Duration, maxMemoryPercent, maxHeapPercent float64) *MemoryHealthCheck {
	return &MemoryHealthCheck{
		name:             name,
		timeout:          timeout,
		maxMemoryPercent: maxMemoryPercent,
		maxHeapPercent:   maxHeapPercent,
	}
}

// Name returns the check name
func (mhc *MemoryHealthCheck) Name() string {
	return mhc.name
}

// Check performs the memory check
func (mhc *MemoryHealthCheck) Check(ctx context.Context) HealthCheckResult {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	memoryPercent := float64(m.Alloc) / float64(m.Sys) * 100
	heapPercent := float64(m.HeapAlloc) / float64(m.HeapSys) * 100

	details := map[string]interface{}{
		"memory_percent": memoryPercent,
		"heap_percent":   heapPercent,
		"alloc_bytes":    m.Alloc,
		"sys_bytes":      m.Sys,
		"heap_alloc":     m.HeapAlloc,
		"heap_sys":       m.HeapSys,
	}

	if memoryPercent > mhc.maxMemoryPercent || heapPercent > mhc.maxHeapPercent {
		return HealthCheckResult{
			Status:  HealthStatusDegraded,
			Message: fmt.Sprintf("High memory usage: %.1f%% memory, %.1f%% heap", memoryPercent, heapPercent),
			Details: details,
		}
	}

	return HealthCheckResult{
		Status:  HealthStatusHealthy,
		Message: fmt.Sprintf("Memory usage normal: %.1f%% memory, %.1f%% heap", memoryPercent, heapPercent),
		Details: details,
	}
}

// Timeout returns the check timeout
func (mhc *MemoryHealthCheck) Timeout() time.Duration {
	return mhc.timeout
}
