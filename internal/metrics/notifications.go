package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"
	"sync"
	"time"
)

// AlertNotifier handles alert notifications
type AlertNotifier struct {
	config *AlertConfig
	client *http.Client

	// Rate limiting
	lastSent map[string]time.Time
	mu       sync.RWMutex
}

// NewAlertNotifier creates a new alert notifier
func NewAlertNotifier(config *AlertConfig) *AlertNotifier {
	return &AlertNotifier{
		config: config,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		lastSent: make(map[string]time.Time),
	}
}

// Start starts the alert notifier
func (an *AlertNotifier) Start() error {
	// Initialize any background processes if needed
	return nil
}

// Stop stops the alert notifier
func (an *AlertNotifier) Stop() error {
	return nil
}

// SendNotification sends notifications for a group of alerts
func (an *AlertNotifier) SendNotification(alerts []*Alert) error {
	if len(alerts) == 0 {
		return nil
	}

	// Find matching route
	route := an.findRoute(alerts[0])
	if route == nil {
		// Use default route if no specific route matches
		route = an.getDefaultRoute()
	}

	// Check rate limiting
	if !an.shouldSend(alerts, route) {
		return nil
	}

	// Find receiver
	receiver := an.findReceiver(route.Receiver)
	if receiver == nil {
		return fmt.Errorf("receiver not found: %s", route.Receiver)
	}

	// Send notifications via all configured channels
	var lastErr error

	// Webhooks
	for _, webhook := range receiver.Webhooks {
		if err := an.sendWebhookNotification(webhook, alerts); err != nil {
			lastErr = err
		}
	}

	// Emails
	for _, email := range receiver.Emails {
		if err := an.sendEmailNotification(email, alerts); err != nil {
			lastErr = err
		}
	}

	// Slack
	for _, slack := range receiver.Slack {
		if err := an.sendSlackNotification(slack, alerts); err != nil {
			lastErr = err
		}
	}

	// Update rate limiting
	an.updateLastSent(alerts)

	return lastErr
}

func (an *AlertNotifier) findRoute(alert *Alert) *AlertRoute {
	for _, route := range an.config.Routes {
		if an.matchesRoute(alert, &route) {
			return &route
		}
	}
	return nil
}

func (an *AlertNotifier) matchesRoute(alert *Alert, route *AlertRoute) bool {
	// Check exact matches
	for key, value := range route.Match {
		if alertValue, exists := alert.Labels[key]; !exists || alertValue != value {
			return false
		}
	}

	// TODO: Implement regex matching for route.MatchRegex

	return true
}

func (an *AlertNotifier) getDefaultRoute() *AlertRoute {
	// Return a default route if none match
	return &AlertRoute{
		Receiver: "default",
		GroupBy:  []string{"alertname"},
	}
}

func (an *AlertNotifier) findReceiver(name string) *AlertReceiver {
	for _, receiver := range an.config.Receivers {
		if receiver.Name == name {
			return &receiver
		}
	}
	return nil
}

func (an *AlertNotifier) shouldSend(alerts []*Alert, route *AlertRoute) bool {
	an.mu.RLock()
	defer an.mu.RUnlock()

	// Check rate limiting
	groupKey := an.generateGroupKey(alerts, route.GroupBy)

	if lastSent, exists := an.lastSent[groupKey]; exists {
		interval := an.config.RepeatInterval
		if route.RepeatInterval != nil {
			interval = *route.RepeatInterval
		}

		if time.Since(lastSent) < interval {
			return false
		}
	}

	return true
}

func (an *AlertNotifier) generateGroupKey(alerts []*Alert, groupBy []string) string {
	if len(groupBy) == 0 {
		return "default"
	}

	// Use first alert's labels for grouping
	if len(alerts) == 0 {
		return "default"
	}

	var keyParts []string
	for _, key := range groupBy {
		if value, exists := alerts[0].Labels[key]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%s=%s", key, value))
		}
	}

	return strings.Join(keyParts, ",")
}

func (an *AlertNotifier) updateLastSent(alerts []*Alert) {
	an.mu.Lock()
	defer an.mu.Unlock()

	now := time.Now()

	// Update for all possible group keys
	for _, route := range an.config.Routes {
		groupKey := an.generateGroupKey(alerts, route.GroupBy)
		an.lastSent[groupKey] = now
	}

	// Update default group key
	defaultGroupKey := an.generateGroupKey(alerts, []string{"alertname"})
	an.lastSent[defaultGroupKey] = now
}

func (an *AlertNotifier) sendWebhookNotification(webhook WebhookConfig, alerts []*Alert) error {
	// Prepare webhook payload
	payload := WebhookPayload{
		Version:  "4",
		GroupKey: an.generateGroupKey(alerts, []string{"alertname"}),
		Status:   an.getGroupStatus(alerts),
		Alerts:   alerts,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	// Create request
	req, err := http.NewRequest("POST", webhook.URL, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add custom headers
	for key, value := range webhook.HTTPConfig.Headers {
		req.Header.Set(key, value)
	}

	// Set timeout
	timeout := 10 * time.Second
	if webhook.HTTPConfig.Timeout > 0 {
		timeout = webhook.HTTPConfig.Timeout
	}

	client := &http.Client{Timeout: timeout}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func (an *AlertNotifier) sendEmailNotification(email EmailConfig, alerts []*Alert) error {
	// Prepare email content
	subject := email.Subject
	if subject == "" {
		subject = fmt.Sprintf("Alert Notification - %d alerts", len(alerts))
	}

	body := email.Body
	if body == "" {
		body = an.generateEmailBody(alerts)
	}

	// Prepare SMTP authentication
	auth := smtp.PlainAuth("", email.SMTPConfig.Username, email.SMTPConfig.Password, email.SMTPConfig.Host)

	// Prepare email message
	msg := fmt.Sprintf("To: %s\r\nSubject: %s\r\n\r\n%s",
		strings.Join(email.To, ","), subject, body)

	// Send email
	addr := fmt.Sprintf("%s:%d", email.SMTPConfig.Host, email.SMTPConfig.Port)
	err := smtp.SendMail(addr, auth, email.From, email.To, []byte(msg))
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}

func (an *AlertNotifier) sendSlackNotification(slack SlackConfig, alerts []*Alert) error {
	// Prepare Slack payload
	payload := SlackPayload{
		Channel:  slack.Channel,
		Username: slack.Username,
		Text:     an.generateSlackText(alerts),
		Attachments: []SlackAttachment{
			{
				Color:  an.getSlackColor(alerts),
				Title:  slack.Title,
				Text:   slack.Text,
				Fields: an.generateSlackFields(alerts),
			},
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal Slack payload: %w", err)
	}

	// Send to Slack webhook
	req, err := http.NewRequest("POST", slack.WebhookURL, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create Slack request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := an.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send Slack notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Slack returned status %d", resp.StatusCode)
	}

	return nil
}

func (an *AlertNotifier) getGroupStatus(alerts []*Alert) string {
	firingCount := 0
	resolvedCount := 0

	for _, alert := range alerts {
		switch alert.Status {
		case AlertStatusFiring:
			firingCount++
		case AlertStatusInactive:
			if alert.EndsAt != nil {
				resolvedCount++
			}
		}
	}

	if firingCount > 0 {
		return "firing"
	}
	if resolvedCount > 0 {
		return "resolved"
	}

	return "inactive"
}

func (an *AlertNotifier) generateEmailBody(alerts []*Alert) string {
	var builder strings.Builder

	builder.WriteString("Alert Notification\n")
	builder.WriteString("==================\n\n")

	for _, alert := range alerts {
		builder.WriteString(fmt.Sprintf("Alert: %s\n", alert.Rule.Name))
		builder.WriteString(fmt.Sprintf("Status: %s\n", alert.Status))
		builder.WriteString(fmt.Sprintf("Severity: %s\n", alert.Rule.Severity))
		builder.WriteString(fmt.Sprintf("Value: %.2f\n", alert.Value))
		builder.WriteString(fmt.Sprintf("Started: %s\n", alert.StartsAt.Format(time.RFC3339)))

		if alert.EndsAt != nil {
			builder.WriteString(fmt.Sprintf("Ended: %s\n", alert.EndsAt.Format(time.RFC3339)))
		}

		if len(alert.Labels) > 0 {
			builder.WriteString("Labels:\n")
			for k, v := range alert.Labels {
				builder.WriteString(fmt.Sprintf("  %s: %s\n", k, v))
			}
		}

		if len(alert.Annotations) > 0 {
			builder.WriteString("Annotations:\n")
			for k, v := range alert.Annotations {
				builder.WriteString(fmt.Sprintf("  %s: %s\n", k, v))
			}
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

func (an *AlertNotifier) generateSlackText(alerts []*Alert) string {
	if len(alerts) == 1 {
		alert := alerts[0]
		return fmt.Sprintf("Alert: %s - %s", alert.Rule.Name, alert.Status)
	}

	firingCount := 0
	resolvedCount := 0

	for _, alert := range alerts {
		switch alert.Status {
		case AlertStatusFiring:
			firingCount++
		case AlertStatusInactive:
			if alert.EndsAt != nil {
				resolvedCount++
			}
		}
	}

	return fmt.Sprintf("%d firing, %d resolved alerts", firingCount, resolvedCount)
}

func (an *AlertNotifier) getSlackColor(alerts []*Alert) string {
	// Determine color based on highest severity
	highestSeverity := AlertSeverityInfo

	for _, alert := range alerts {
		if alert.Status == AlertStatusFiring {
			switch alert.Rule.Severity {
			case AlertSeverityCritical:
				highestSeverity = AlertSeverityCritical
			case AlertSeverityError:
				if highestSeverity != AlertSeverityCritical {
					highestSeverity = AlertSeverityError
				}
			case AlertSeverityWarning:
				if highestSeverity != AlertSeverityCritical && highestSeverity != AlertSeverityError {
					highestSeverity = AlertSeverityWarning
				}
			}
		}
	}

	switch highestSeverity {
	case AlertSeverityCritical:
		return "danger"
	case AlertSeverityError:
		return "warning"
	case AlertSeverityWarning:
		return "warning"
	default:
		return "good"
	}
}

func (an *AlertNotifier) generateSlackFields(alerts []*Alert) []SlackField {
	var fields []SlackField

	for _, alert := range alerts {
		fields = append(fields, SlackField{
			Title: alert.Rule.Name,
			Value: fmt.Sprintf("Status: %s\nValue: %.2f\nSeverity: %s",
				alert.Status, alert.Value, alert.Rule.Severity),
			Short: true,
		})
	}

	return fields
}

// Webhook payload structures

// WebhookPayload represents the payload sent to webhooks
type WebhookPayload struct {
	Version  string   `json:"version"`
	GroupKey string   `json:"groupKey"`
	Status   string   `json:"status"`
	Alerts   []*Alert `json:"alerts"`
}

// Slack payload structures

// SlackPayload represents the payload sent to Slack
type SlackPayload struct {
	Channel     string            `json:"channel,omitempty"`
	Username    string            `json:"username,omitempty"`
	Text        string            `json:"text"`
	Attachments []SlackAttachment `json:"attachments,omitempty"`
}

// SlackAttachment represents a Slack message attachment
type SlackAttachment struct {
	Color  string       `json:"color,omitempty"`
	Title  string       `json:"title,omitempty"`
	Text   string       `json:"text,omitempty"`
	Fields []SlackField `json:"fields,omitempty"`
}

// SlackField represents a field in a Slack attachment
type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// TestNotifier sends a test notification
func (an *AlertNotifier) SendTestNotification(receiverName string) error {
	receiver := an.findReceiver(receiverName)
	if receiver == nil {
		return fmt.Errorf("receiver not found: %s", receiverName)
	}

	// Create a test alert
	testAlert := &Alert{
		Rule: &AlertRule{
			Name:     "TestAlert",
			Severity: AlertSeverityInfo,
		},
		Status:   AlertStatusFiring,
		Value:    42.0,
		StartsAt: time.Now(),
		Labels: map[string]string{
			"test": "true",
		},
		Annotations: map[string]string{
			"summary":     "This is a test alert",
			"description": "This alert is used to test notification configuration",
		},
	}

	return an.SendNotification([]*Alert{testAlert})
}
