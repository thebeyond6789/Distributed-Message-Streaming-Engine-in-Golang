package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AlertSeverity represents the severity of an alert
type AlertSeverity string

const (
	AlertSeverityInfo     AlertSeverity = "info"
	AlertSeverityWarning  AlertSeverity = "warning"
	AlertSeverityError    AlertSeverity = "error"
	AlertSeverityCritical AlertSeverity = "critical"
)

// AlertStatus represents the status of an alert
type AlertStatus string

const (
	AlertStatusFiring   AlertStatus = "firing"
	AlertStatusPending  AlertStatus = "pending"
	AlertStatusInactive AlertStatus = "inactive"
)

// AlertRule defines conditions for triggering alerts
type AlertRule struct {
	Name        string            `json:"name"`
	Query       string            `json:"query"`
	Condition   AlertCondition    `json:"condition"`
	Threshold   float64           `json:"threshold"`
	Duration    time.Duration     `json:"duration"`
	Severity    AlertSeverity     `json:"severity"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`

	// Internal state
	lastEvaluated time.Time
	firingTime    time.Time
	status        AlertStatus
	mu            sync.RWMutex
}

// AlertCondition represents different types of alert conditions
type AlertCondition string

const (
	ConditionGreaterThan    AlertCondition = "greater_than"
	ConditionLessThan       AlertCondition = "less_than"
	ConditionEquals         AlertCondition = "equals"
	ConditionNotEquals      AlertCondition = "not_equals"
	ConditionGreaterOrEqual AlertCondition = "greater_or_equal"
	ConditionLessOrEqual    AlertCondition = "less_or_equal"
	ConditionAbsent         AlertCondition = "absent"
	ConditionPresent        AlertCondition = "present"
)

// Alert represents an active alert
type Alert struct {
	Rule         *AlertRule        `json:"rule"`
	Status       AlertStatus       `json:"status"`
	Value        float64           `json:"value"`
	StartsAt     time.Time         `json:"starts_at"`
	EndsAt       *time.Time        `json:"ends_at,omitempty"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	GeneratorURL string            `json:"generator_url,omitempty"`
}

// AlertConfig represents alert manager configuration
type AlertConfig struct {
	EvaluationInterval time.Duration   `json:"evaluation_interval"`
	GroupWait          time.Duration   `json:"group_wait"`
	GroupInterval      time.Duration   `json:"group_interval"`
	RepeatInterval     time.Duration   `json:"repeat_interval"`
	Routes             []AlertRoute    `json:"routes"`
	Receivers          []AlertReceiver `json:"receivers"`
	InhibitRules       []InhibitRule   `json:"inhibit_rules"`
}

// DefaultAlertConfig returns default alert configuration
func DefaultAlertConfig() *AlertConfig {
	return &AlertConfig{
		EvaluationInterval: 15 * time.Second,
		GroupWait:          10 * time.Second,
		GroupInterval:      10 * time.Second,
		RepeatInterval:     1 * time.Hour,
		Routes:             []AlertRoute{},
		Receivers:          []AlertReceiver{},
		InhibitRules:       []InhibitRule{},
	}
}

// AlertRoute defines routing rules for alerts
type AlertRoute struct {
	Match          map[string]string `json:"match"`
	MatchRegex     map[string]string `json:"match_regex"`
	Receiver       string            `json:"receiver"`
	GroupBy        []string          `json:"group_by"`
	Continue       bool              `json:"continue"`
	Routes         []AlertRoute      `json:"routes"`
	GroupWait      *time.Duration    `json:"group_wait,omitempty"`
	GroupInterval  *time.Duration    `json:"group_interval,omitempty"`
	RepeatInterval *time.Duration    `json:"repeat_interval,omitempty"`
}

// AlertReceiver defines how to send alert notifications
type AlertReceiver struct {
	Name     string          `json:"name"`
	Webhooks []WebhookConfig `json:"webhooks,omitempty"`
	Emails   []EmailConfig   `json:"emails,omitempty"`
	Slack    []SlackConfig   `json:"slack,omitempty"`
}

// WebhookConfig represents webhook notification configuration
type WebhookConfig struct {
	URL          string     `json:"url"`
	HTTPConfig   HTTPConfig `json:"http_config,omitempty"`
	SendResolved bool       `json:"send_resolved"`
	MaxAlerts    int        `json:"max_alerts"`
	Title        string     `json:"title,omitempty"`
	Text         string     `json:"text,omitempty"`
}

// EmailConfig represents email notification configuration
type EmailConfig struct {
	To           []string   `json:"to"`
	From         string     `json:"from"`
	Subject      string     `json:"subject"`
	Body         string     `json:"body"`
	HTML         string     `json:"html,omitempty"`
	SMTPConfig   SMTPConfig `json:"smtp_config"`
	SendResolved bool       `json:"send_resolved"`
}

// SlackConfig represents Slack notification configuration
type SlackConfig struct {
	Channel      string `json:"channel"`
	Username     string `json:"username,omitempty"`
	Color        string `json:"color,omitempty"`
	Title        string `json:"title,omitempty"`
	Text         string `json:"text,omitempty"`
	WebhookURL   string `json:"webhook_url"`
	SendResolved bool   `json:"send_resolved"`
}

// HTTPConfig represents HTTP configuration
type HTTPConfig struct {
	Timeout time.Duration     `json:"timeout"`
	Headers map[string]string `json:"headers,omitempty"`
}

// SMTPConfig represents SMTP configuration
type SMTPConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	UseTLS   bool   `json:"use_tls"`
}

// InhibitRule defines rules for inhibiting alerts
type InhibitRule struct {
	SourceMatch      map[string]string `json:"source_match"`
	SourceMatchRegex map[string]string `json:"source_match_regex"`
	TargetMatch      map[string]string `json:"target_match"`
	TargetMatchRegex map[string]string `json:"target_match_regex"`
	Equal            []string          `json:"equal"`
}

// AlertManager manages alert rules and notifications
type AlertManager struct {
	config       *AlertConfig
	rules        map[string]*AlertRule
	activeAlerts map[string]*Alert

	// Notification
	notifier *AlertNotifier

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// NewAlertManager creates a new alert manager
func NewAlertManager(config *AlertConfig) *AlertManager {
	if config == nil {
		config = DefaultAlertConfig()
	}

	return &AlertManager{
		config:       config,
		rules:        make(map[string]*AlertRule),
		activeAlerts: make(map[string]*Alert),
		notifier:     NewAlertNotifier(config),
		stopCh:       make(chan struct{}),
	}
}

// Start starts the alert manager
func (am *AlertManager) Start() error {
	am.wg.Add(1)
	go am.evaluationLoop()

	return am.notifier.Start()
}

// Stop stops the alert manager
func (am *AlertManager) Stop() error {
	close(am.stopCh)
	am.wg.Wait()

	return am.notifier.Stop()
}

// RegisterRule registers an alert rule
func (am *AlertManager) RegisterRule(rule *AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.rules[rule.Name] = rule
}

// UnregisterRule unregisters an alert rule
func (am *AlertManager) UnregisterRule(name string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	delete(am.rules, name)

	// Remove associated active alerts
	for alertID, alert := range am.activeAlerts {
		if alert.Rule.Name == name {
			delete(am.activeAlerts, alertID)
		}
	}
}

// EvaluateAlerts evaluates all rules against current metrics
func (am *AlertManager) EvaluateAlerts(metrics []Metric) {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now()

	for _, rule := range am.rules {
		am.evaluateRule(rule, metrics, now)
	}

	// Send notifications for new/resolved alerts
	am.processAlerts()
}

// GetActiveAlerts returns all active alerts
func (am *AlertManager) GetActiveAlerts() []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	alerts := make([]*Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, alert)
	}

	// Sort by severity and start time
	sort.Slice(alerts, func(i, j int) bool {
		if alerts[i].Rule.Severity != alerts[j].Rule.Severity {
			return severityWeight(alerts[i].Rule.Severity) > severityWeight(alerts[j].Rule.Severity)
		}
		return alerts[i].StartsAt.Before(alerts[j].StartsAt)
	})

	return alerts
}

// GetStatus returns alert manager status
func (am *AlertManager) GetStatus() *AlertManagerStatus {
	am.mu.RLock()
	defer am.mu.RUnlock()

	status := &AlertManagerStatus{
		RuleCount:    len(am.rules),
		ActiveAlerts: len(am.activeAlerts),
		Rules:        make([]*AlertRule, 0, len(am.rules)),
	}

	for _, rule := range am.rules {
		status.Rules = append(status.Rules, rule)
	}

	return status
}

func (am *AlertManager) evaluationLoop() {
	defer am.wg.Done()

	ticker := time.NewTicker(am.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-am.stopCh:
			return
		case <-ticker.C:
			// Alert evaluation is triggered externally via EvaluateAlerts
			// This loop handles cleanup and maintenance
			am.cleanupExpiredAlerts()
		}
	}
}

func (am *AlertManager) evaluateRule(rule *AlertRule, metrics []Metric, now time.Time) {
	rule.mu.Lock()
	defer rule.mu.Unlock()

	rule.lastEvaluated = now

	// Find metrics matching the rule query
	matchingMetrics := am.findMatchingMetrics(rule.Query, metrics)

	for _, metric := range matchingMetrics {
		value := am.extractValue(metric)
		shouldFire := am.evaluateCondition(rule.Condition, value, rule.Threshold)

		alertID := am.generateAlertID(rule, metric)
		existingAlert := am.activeAlerts[alertID]

		if shouldFire {
			if existingAlert == nil {
				// New alert
				alert := &Alert{
					Rule:        rule,
					Status:      AlertStatusPending,
					Value:       value,
					StartsAt:    now,
					Labels:      am.mergeLabels(rule.Labels, metric.Labels()),
					Annotations: rule.Annotations,
				}

				am.activeAlerts[alertID] = alert
				rule.firingTime = now
				rule.status = AlertStatusPending
			} else {
				// Update existing alert
				existingAlert.Value = value

				// Check if alert should transition from pending to firing
				if existingAlert.Status == AlertStatusPending &&
					now.Sub(existingAlert.StartsAt) >= rule.Duration {
					existingAlert.Status = AlertStatusFiring
					rule.status = AlertStatusFiring
				}
			}
		} else {
			if existingAlert != nil && existingAlert.Status == AlertStatusFiring {
				// Resolve alert
				endTime := now
				existingAlert.EndsAt = &endTime
				existingAlert.Status = AlertStatusInactive
				rule.status = AlertStatusInactive

				// Keep resolved alert for a while for notification
				go func() {
					time.Sleep(5 * time.Minute)
					am.mu.Lock()
					delete(am.activeAlerts, alertID)
					am.mu.Unlock()
				}()
			} else if existingAlert != nil && existingAlert.Status == AlertStatusPending {
				// Remove pending alert that no longer fires
				delete(am.activeAlerts, alertID)
				rule.status = AlertStatusInactive
			}
		}
	}
}

func (am *AlertManager) findMatchingMetrics(query string, metrics []Metric) []Metric {
	var matching []Metric

	// Simple query matching - in a real implementation, this would be more sophisticated
	for _, metric := range metrics {
		if am.matchesQuery(metric, query) {
			matching = append(matching, metric)
		}
	}

	return matching
}

func (am *AlertManager) matchesQuery(metric Metric, query string) bool {
	// Simplified query matching
	// In practice, this would support more complex queries
	return metric.Name() == query
}

func (am *AlertManager) extractValue(metric Metric) float64 {
	switch v := metric.Value().(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	case map[string]interface{}:
		// For histograms/timers, extract a representative value
		if count, exists := v["count"]; exists {
			if countVal, ok := count.(int64); ok {
				return float64(countVal)
			}
		}
		if sum, exists := v["sum"]; exists {
			if sumVal, ok := sum.(float64); ok {
				return sumVal
			}
		}
	}
	return 0
}

func (am *AlertManager) evaluateCondition(condition AlertCondition, value, threshold float64) bool {
	switch condition {
	case ConditionGreaterThan:
		return value > threshold
	case ConditionLessThan:
		return value < threshold
	case ConditionEquals:
		return math.Abs(value-threshold) < 1e-9
	case ConditionNotEquals:
		return math.Abs(value-threshold) >= 1e-9
	case ConditionGreaterOrEqual:
		return value >= threshold
	case ConditionLessOrEqual:
		return value <= threshold
	case ConditionAbsent:
		return math.IsNaN(value)
	case ConditionPresent:
		return !math.IsNaN(value)
	default:
		return false
	}
}

func (am *AlertManager) generateAlertID(rule *AlertRule, metric Metric) string {
	// Generate a unique ID for the alert based on rule and metric labels
	id := fmt.Sprintf("%s:", rule.Name)

	labels := metric.Labels()
	if labels != nil {
		var keys []string
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			id += fmt.Sprintf("%s=%s,", k, labels[k])
		}
	}

	return id
}

func (am *AlertManager) mergeLabels(ruleLabels, metricLabels map[string]string) map[string]string {
	merged := make(map[string]string)

	for k, v := range metricLabels {
		merged[k] = v
	}

	for k, v := range ruleLabels {
		merged[k] = v
	}

	return merged
}

func (am *AlertManager) processAlerts() {
	// Group alerts and send notifications
	alertGroups := am.groupAlerts()

	for _, group := range alertGroups {
		am.notifier.SendNotification(group)
	}
}

func (am *AlertManager) groupAlerts() [][]*Alert {
	// Simple grouping by severity
	groups := make(map[AlertSeverity][]*Alert)

	for _, alert := range am.activeAlerts {
		if alert.Status == AlertStatusFiring ||
			(alert.Status == AlertStatusInactive && alert.EndsAt != nil) {
			groups[alert.Rule.Severity] = append(groups[alert.Rule.Severity], alert)
		}
	}

	var result [][]*Alert
	for _, group := range groups {
		if len(group) > 0 {
			result = append(result, group)
		}
	}

	return result
}

func (am *AlertManager) cleanupExpiredAlerts() {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now()

	for alertID, alert := range am.activeAlerts {
		// Remove resolved alerts after 5 minutes
		if alert.Status == AlertStatusInactive &&
			alert.EndsAt != nil &&
			now.Sub(*alert.EndsAt) > 5*time.Minute {
			delete(am.activeAlerts, alertID)
		}
	}
}

func severityWeight(severity AlertSeverity) int {
	switch severity {
	case AlertSeverityCritical:
		return 4
	case AlertSeverityError:
		return 3
	case AlertSeverityWarning:
		return 2
	case AlertSeverityInfo:
		return 1
	default:
		return 0
	}
}

// AlertManagerStatus represents the status of the alert manager
type AlertManagerStatus struct {
	RuleCount    int          `json:"rule_count"`
	ActiveAlerts int          `json:"active_alerts"`
	Rules        []*AlertRule `json:"rules"`
}

// Common alert rules

// CreateHighCPUAlert creates a high CPU usage alert rule
func CreateHighCPUAlert(threshold float64) *AlertRule {
	return &AlertRule{
		Name:      "HighCPUUsage",
		Query:     "cpu_usage_percent",
		Condition: ConditionGreaterThan,
		Threshold: threshold,
		Duration:  5 * time.Minute,
		Severity:  AlertSeverityWarning,
		Labels: map[string]string{
			"component": "system",
		},
		Annotations: map[string]string{
			"summary":     "High CPU usage detected",
			"description": fmt.Sprintf("CPU usage is above %.1f%%", threshold),
		},
	}
}

// CreateHighMemoryAlert creates a high memory usage alert rule
func CreateHighMemoryAlert(threshold float64) *AlertRule {
	return &AlertRule{
		Name:      "HighMemoryUsage",
		Query:     "memory_usage_bytes",
		Condition: ConditionGreaterThan,
		Threshold: threshold,
		Duration:  5 * time.Minute,
		Severity:  AlertSeverityWarning,
		Labels: map[string]string{
			"component": "system",
		},
		Annotations: map[string]string{
			"summary":     "High memory usage detected",
			"description": fmt.Sprintf("Memory usage is above %.0f bytes", threshold),
		},
	}
}

// CreateHighErrorRateAlert creates a high error rate alert rule
func CreateHighErrorRateAlert(threshold float64) *AlertRule {
	return &AlertRule{
		Name:      "HighErrorRate",
		Query:     "error_rate_percent",
		Condition: ConditionGreaterThan,
		Threshold: threshold,
		Duration:  2 * time.Minute,
		Severity:  AlertSeverityError,
		Labels: map[string]string{
			"component": "application",
		},
		Annotations: map[string]string{
			"summary":     "High error rate detected",
			"description": fmt.Sprintf("Error rate is above %.1f%%", threshold),
		},
	}
}

// CreateServiceDownAlert creates a service down alert rule
func CreateServiceDownAlert() *AlertRule {
	return &AlertRule{
		Name:      "ServiceDown",
		Query:     "up",
		Condition: ConditionLessThan,
		Threshold: 1,
		Duration:  1 * time.Minute,
		Severity:  AlertSeverityCritical,
		Labels: map[string]string{
			"component": "service",
		},
		Annotations: map[string]string{
			"summary":     "Service is down",
			"description": "Service is not responding",
		},
	}
}
