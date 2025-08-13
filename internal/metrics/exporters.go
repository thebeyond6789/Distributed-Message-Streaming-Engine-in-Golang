package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// PrometheusExporter exports metrics in Prometheus format
type PrometheusExporter struct {
	endpoint string
	client   *http.Client
	jobName  string
	instance string
	mu       sync.RWMutex
}

// NewPrometheusExporter creates a new Prometheus exporter
func NewPrometheusExporter(endpoint, jobName, instance string) *PrometheusExporter {
	return &PrometheusExporter{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		jobName:  jobName,
		instance: instance,
	}
}

// Export exports metrics to Prometheus pushgateway
func (pe *PrometheusExporter) Export(metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	prometheusData := pe.formatPrometheusMetrics(metrics)

	url := fmt.Sprintf("%s/metrics/job/%s/instance/%s", pe.endpoint, pe.jobName, pe.instance)

	req, err := http.NewRequest("POST", url, strings.NewReader(prometheusData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain")

	resp, err := pe.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("prometheus export failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Close closes the exporter
func (pe *PrometheusExporter) Close() error {
	return nil
}

func (pe *PrometheusExporter) formatPrometheusMetrics(metrics []Metric) string {
	var builder strings.Builder

	// Group metrics by name
	metricGroups := make(map[string][]Metric)
	for _, metric := range metrics {
		name := pe.sanitizeMetricName(metric.Name())
		metricGroups[name] = append(metricGroups[name], metric)
	}

	// Sort metric names for consistent output
	var sortedNames []string
	for name := range metricGroups {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	for _, name := range sortedNames {
		groupMetrics := metricGroups[name]

		// Write help comment
		builder.WriteString(fmt.Sprintf("# HELP %s %s\n", name, name))

		// Write type comment
		metricType := pe.prometheusType(groupMetrics[0].Type())
		builder.WriteString(fmt.Sprintf("# TYPE %s %s\n", name, metricType))

		// Write metric values
		for _, metric := range groupMetrics {
			pe.writePrometheusMetric(&builder, name, metric)
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

func (pe *PrometheusExporter) writePrometheusMetric(builder *strings.Builder, name string, metric Metric) {
	labels := pe.formatLabels(metric.Labels())

	switch metric.Type() {
	case MetricTypeCounter:
		value := metric.Value().(int64)
		builder.WriteString(fmt.Sprintf("%s%s %d\n", name, labels, value))

	case MetricTypeGauge:
		value := metric.Value().(float64)
		builder.WriteString(fmt.Sprintf("%s%s %f\n", name, labels, value))

	case MetricTypeHistogram:
		histData := metric.Value().(map[string]interface{})
		buckets := histData["buckets"].([]HistogramBucket)
		count := histData["count"].(int64)
		sum := histData["sum"].(float64)

		// Write buckets
		for _, bucket := range buckets {
			bucketLabels := pe.addLabel(labels, "le", fmt.Sprintf("%f", bucket.UpperBound))
			builder.WriteString(fmt.Sprintf("%s_bucket%s %d\n", name, bucketLabels, bucket.Count))
		}

		// Write +Inf bucket
		infLabels := pe.addLabel(labels, "le", "+Inf")
		builder.WriteString(fmt.Sprintf("%s_bucket%s %d\n", name, infLabels, count))

		// Write count and sum
		builder.WriteString(fmt.Sprintf("%s_count%s %d\n", name, labels, count))
		builder.WriteString(fmt.Sprintf("%s_sum%s %f\n", name, labels, sum))

	case MetricTypeTimer:
		timerData := metric.Value().(map[string]interface{})
		count := timerData["count"].(int64)
		sum := timerData["sum"].(time.Duration)
		percentiles := timerData["percentiles"].(map[float64]time.Duration)

		// Write count and sum
		builder.WriteString(fmt.Sprintf("%s_count%s %d\n", name, labels, count))
		builder.WriteString(fmt.Sprintf("%s_sum%s %f\n", name, labels, sum.Seconds()))

		// Write percentiles
		for p, duration := range percentiles {
			percentileLabels := pe.addLabel(labels, "quantile", fmt.Sprintf("%f", p))
			builder.WriteString(fmt.Sprintf("%s%s %f\n", name, percentileLabels, duration.Seconds()))
		}
	}
}

func (pe *PrometheusExporter) sanitizeMetricName(name string) string {
	// Replace invalid characters with underscores
	result := strings.ReplaceAll(name, "-", "_")
	result = strings.ReplaceAll(result, ".", "_")
	result = strings.ReplaceAll(result, " ", "_")
	return result
}

func (pe *PrometheusExporter) prometheusType(metricType MetricType) string {
	switch metricType {
	case MetricTypeCounter:
		return "counter"
	case MetricTypeGauge:
		return "gauge"
	case MetricTypeHistogram:
		return "histogram"
	case MetricTypeTimer:
		return "summary"
	default:
		return "untyped"
	}
}

func (pe *PrometheusExporter) formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	var labelPairs []string
	for key, value := range labels {
		labelPairs = append(labelPairs, fmt.Sprintf(`%s="%s"`, key, value))
	}

	sort.Strings(labelPairs)
	return fmt.Sprintf("{%s}", strings.Join(labelPairs, ","))
}

func (pe *PrometheusExporter) addLabel(existingLabels, key, value string) string {
	if existingLabels == "" {
		return fmt.Sprintf(`{%s="%s"}`, key, value)
	}

	// Remove the closing brace and add the new label
	withoutBrace := existingLabels[:len(existingLabels)-1]
	return fmt.Sprintf(`%s,%s="%s"}`, withoutBrace, key, value)
}

// JSONExporter exports metrics in JSON format
type JSONExporter struct {
	endpoint string
	client   *http.Client
	mu       sync.RWMutex
}

// NewJSONExporter creates a new JSON exporter
func NewJSONExporter(endpoint string) *JSONExporter {
	return &JSONExporter{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Export exports metrics as JSON
func (je *JSONExporter) Export(metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	jsonData := je.formatJSONMetrics(metrics)

	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	req, err := http.NewRequest("POST", je.endpoint, bytes.NewReader(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := je.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("JSON export failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Close closes the exporter
func (je *JSONExporter) Close() error {
	return nil
}

func (je *JSONExporter) formatJSONMetrics(metrics []Metric) map[string]interface{} {
	result := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"metrics":   make([]map[string]interface{}, 0, len(metrics)),
	}

	for _, metric := range metrics {
		metricData := map[string]interface{}{
			"name":   metric.Name(),
			"type":   string(metric.Type()),
			"value":  metric.Value(),
			"labels": metric.Labels(),
		}

		result["metrics"] = append(result["metrics"].([]map[string]interface{}), metricData)
	}

	return result
}

// ConsoleExporter exports metrics to console/stdout
type ConsoleExporter struct {
	pretty bool
	mu     sync.RWMutex
}

// NewConsoleExporter creates a new console exporter
func NewConsoleExporter(pretty bool) *ConsoleExporter {
	return &ConsoleExporter{
		pretty: pretty,
	}
}

// Export exports metrics to console
func (ce *ConsoleExporter) Export(metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	if ce.pretty {
		return ce.exportPretty(metrics)
	}

	return ce.exportJSON(metrics)
}

// Close closes the exporter
func (ce *ConsoleExporter) Close() error {
	return nil
}

func (ce *ConsoleExporter) exportPretty(metrics []Metric) error {
	fmt.Printf("=== Metrics Report (%s) ===\n", time.Now().Format(time.RFC3339))

	// Group by type
	typeGroups := make(map[MetricType][]Metric)
	for _, metric := range metrics {
		typeGroups[metric.Type()] = append(typeGroups[metric.Type()], metric)
	}

	// Print each type group
	for metricType, groupMetrics := range typeGroups {
		fmt.Printf("\n--- %s ---\n", strings.ToUpper(string(metricType)))

		for _, metric := range groupMetrics {
			labels := ""
			if len(metric.Labels()) > 0 {
				var labelPairs []string
				for k, v := range metric.Labels() {
					labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
				}
				labels = fmt.Sprintf(" {%s}", strings.Join(labelPairs, ", "))
			}

			fmt.Printf("  %s%s: %v\n", metric.Name(), labels, metric.Value())
		}
	}

	fmt.Printf("\n=== End Report ===\n\n")
	return nil
}

func (ce *ConsoleExporter) exportJSON(metrics []Metric) error {
	jsonData := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"metrics":   make([]map[string]interface{}, 0, len(metrics)),
	}

	for _, metric := range metrics {
		metricData := map[string]interface{}{
			"name":   metric.Name(),
			"type":   string(metric.Type()),
			"value":  metric.Value(),
			"labels": metric.Labels(),
		}

		jsonData["metrics"] = append(jsonData["metrics"].([]map[string]interface{}), metricData)
	}

	jsonBytes, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	fmt.Println(string(jsonBytes))
	return nil
}

// FileExporter exports metrics to a file
type FileExporter struct {
	filename string
	format   string // "json" or "prometheus"
	mu       sync.RWMutex
}

// NewFileExporter creates a new file exporter
func NewFileExporter(filename, format string) *FileExporter {
	return &FileExporter{
		filename: filename,
		format:   format,
	}
}

// Export exports metrics to file
func (fe *FileExporter) Export(metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	fe.mu.Lock()
	defer fe.mu.Unlock()

	var data string
	var err error

	switch fe.format {
	case "prometheus":
		pe := NewPrometheusExporter("", "", "")
		data = pe.formatPrometheusMetrics(metrics)
	case "json":
		je := NewJSONExporter("")
		jsonData := je.formatJSONMetrics(metrics)
		jsonBytes, err := json.MarshalIndent(jsonData, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal metrics: %w", err)
		}
		data = string(jsonBytes)
	default:
		return fmt.Errorf("unsupported format: %s", fe.format)
	}

	// Write to file
	file, err := os.Create(fe.filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(data)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}

// Close closes the exporter
func (fe *FileExporter) Close() error {
	return nil
}

// HTTPExporter serves metrics over HTTP
type HTTPExporter struct {
	addr     string
	server   *http.Server
	registry MetricRegistry
	mu       sync.RWMutex
	started  bool
}

// NewHTTPExporter creates a new HTTP exporter
func NewHTTPExporter(addr string, registry MetricRegistry) *HTTPExporter {
	he := &HTTPExporter{
		addr:     addr,
		registry: registry,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", he.handleMetrics)
	mux.HandleFunc("/health", he.handleHealth)

	he.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return he
}

// Start starts the HTTP server
func (he *HTTPExporter) Start() error {
	he.mu.Lock()
	defer he.mu.Unlock()

	if he.started {
		return fmt.Errorf("HTTP exporter already started")
	}

	go func() {
		if err := he.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP exporter error: %v\n", err)
		}
	}()

	he.started = true
	return nil
}

// Export is a no-op for HTTP exporter (serves on demand)
func (he *HTTPExporter) Export(metrics []Metric) error {
	// HTTP exporter serves metrics on demand, so this is a no-op
	return nil
}

// Close closes the HTTP server
func (he *HTTPExporter) Close() error {
	he.mu.Lock()
	defer he.mu.Unlock()

	if !he.started {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	he.started = false
	return he.server.Shutdown(ctx)
}

func (he *HTTPExporter) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := he.registry.GetAllMetrics()

	// Check Accept header to determine format
	acceptHeader := r.Header.Get("Accept")

	if strings.Contains(acceptHeader, "application/json") {
		he.serveJSON(w, metrics)
	} else {
		he.servePrometheus(w, metrics)
	}
}

func (he *HTTPExporter) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	}

	json.NewEncoder(w).Encode(response)
}

func (he *HTTPExporter) servePrometheus(w http.ResponseWriter, metrics []Metric) {
	w.Header().Set("Content-Type", "text/plain")

	pe := NewPrometheusExporter("", "", "")
	data := pe.formatPrometheusMetrics(metrics)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(data))
}

func (he *HTTPExporter) serveJSON(w http.ResponseWriter, metrics []Metric) {
	w.Header().Set("Content-Type", "application/json")

	je := NewJSONExporter("")
	jsonData := je.formatJSONMetrics(metrics)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jsonData)
}
