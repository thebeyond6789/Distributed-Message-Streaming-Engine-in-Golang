package network

import (
	"sync"
	"time"
)

// RateLimiter implements a token bucket rate limiter
type RateLimiter struct {
	mu         sync.Mutex
	tokens     int
	maxTokens  int
	refillRate int
	window     time.Duration
	lastRefill time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(maxRequests int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		tokens:     maxRequests,
		maxTokens:  maxRequests,
		refillRate: maxRequests,
		window:     window,
		lastRefill: time.Now(),
	}
}

// Allow checks if a request is allowed under the rate limit
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// Calculate how many tokens to add based on elapsed time
	elapsed := now.Sub(rl.lastRefill)
	tokensToAdd := int(elapsed.Nanoseconds() * int64(rl.refillRate) / int64(rl.window.Nanoseconds()))

	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd
		if rl.tokens > rl.maxTokens {
			rl.tokens = rl.maxTokens
		}
		rl.lastRefill = now
	}

	// Check if we have tokens available
	if rl.tokens > 0 {
		rl.tokens--
		return true
	}

	return false
}

// AllowN checks if N requests are allowed under the rate limit
func (rl *RateLimiter) AllowN(n int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// Calculate how many tokens to add based on elapsed time
	elapsed := now.Sub(rl.lastRefill)
	tokensToAdd := int(elapsed.Nanoseconds() * int64(rl.refillRate) / int64(rl.window.Nanoseconds()))

	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd
		if rl.tokens > rl.maxTokens {
			rl.tokens = rl.maxTokens
		}
		rl.lastRefill = now
	}

	// Check if we have enough tokens available
	if rl.tokens >= n {
		rl.tokens -= n
		return true
	}

	return false
}

// Tokens returns the current number of available tokens
func (rl *RateLimiter) Tokens() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// Calculate how many tokens to add based on elapsed time
	elapsed := now.Sub(rl.lastRefill)
	tokensToAdd := int(elapsed.Nanoseconds() * int64(rl.refillRate) / int64(rl.window.Nanoseconds()))

	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd
		if rl.tokens > rl.maxTokens {
			rl.tokens = rl.maxTokens
		}
		rl.lastRefill = now
	}

	return rl.tokens
}

// SetRate updates the rate limit parameters
func (rl *RateLimiter) SetRate(maxRequests int, window time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.maxTokens = maxRequests
	rl.refillRate = maxRequests
	rl.window = window

	// Adjust current tokens if necessary
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
}

// Reset resets the rate limiter to full capacity
func (rl *RateLimiter) Reset() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.tokens = rl.maxTokens
	rl.lastRefill = time.Now()
}

// WaitForToken waits until a token becomes available
func (rl *RateLimiter) WaitForToken() {
	for !rl.Allow() {
		time.Sleep(time.Millisecond)
	}
}

// WaitForTokens waits until N tokens become available
func (rl *RateLimiter) WaitForTokens(n int) {
	for !rl.AllowN(n) {
		time.Sleep(time.Millisecond)
	}
}

// PerConnectionRateLimiter manages rate limiters per connection
type PerConnectionRateLimiter struct {
	limiters    map[string]*RateLimiter
	mu          sync.RWMutex
	maxRequests int
	window      time.Duration

	// Cleanup
	cleanupTicker *time.Ticker
	done          chan struct{}
}

// NewPerConnectionRateLimiter creates a new per-connection rate limiter
func NewPerConnectionRateLimiter(maxRequests int, window time.Duration) *PerConnectionRateLimiter {
	pcrl := &PerConnectionRateLimiter{
		limiters:    make(map[string]*RateLimiter),
		maxRequests: maxRequests,
		window:      window,
		done:        make(chan struct{}),
	}

	// Start cleanup routine
	pcrl.cleanupTicker = time.NewTicker(5 * time.Minute)
	go pcrl.cleanup()

	return pcrl
}

// Allow checks if a request is allowed for the given connection
func (pcrl *PerConnectionRateLimiter) Allow(connID string) bool {
	limiter := pcrl.getLimiter(connID)
	return limiter.Allow()
}

// AllowN checks if N requests are allowed for the given connection
func (pcrl *PerConnectionRateLimiter) AllowN(connID string, n int) bool {
	limiter := pcrl.getLimiter(connID)
	return limiter.AllowN(n)
}

// RemoveConnection removes the rate limiter for a connection
func (pcrl *PerConnectionRateLimiter) RemoveConnection(connID string) {
	pcrl.mu.Lock()
	defer pcrl.mu.Unlock()

	delete(pcrl.limiters, connID)
}

// Close closes the per-connection rate limiter
func (pcrl *PerConnectionRateLimiter) Close() {
	close(pcrl.done)
	if pcrl.cleanupTicker != nil {
		pcrl.cleanupTicker.Stop()
	}
}

// GetStats returns statistics about the rate limiters
func (pcrl *PerConnectionRateLimiter) GetStats() map[string]interface{} {
	pcrl.mu.RLock()
	defer pcrl.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_connections"] = len(pcrl.limiters)

	connectionStats := make(map[string]int)
	for connID, limiter := range pcrl.limiters {
		connectionStats[connID] = limiter.Tokens()
	}
	stats["connection_tokens"] = connectionStats

	return stats
}

// Private methods

func (pcrl *PerConnectionRateLimiter) getLimiter(connID string) *RateLimiter {
	pcrl.mu.RLock()
	limiter, exists := pcrl.limiters[connID]
	pcrl.mu.RUnlock()

	if exists {
		return limiter
	}

	// Create new limiter
	pcrl.mu.Lock()
	defer pcrl.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists := pcrl.limiters[connID]; exists {
		return limiter
	}

	limiter = NewRateLimiter(pcrl.maxRequests, pcrl.window)
	pcrl.limiters[connID] = limiter

	return limiter
}

func (pcrl *PerConnectionRateLimiter) cleanup() {
	for {
		select {
		case <-pcrl.done:
			return
		case <-pcrl.cleanupTicker.C:
			pcrl.performCleanup()
		}
	}
}

func (pcrl *PerConnectionRateLimiter) performCleanup() {
	pcrl.mu.Lock()
	defer pcrl.mu.Unlock()

	// Remove limiters for connections that haven't been used recently
	now := time.Now()
	toRemove := make([]string, 0)

	for connID, limiter := range pcrl.limiters {
		// If limiter hasn't been accessed recently and has full tokens, remove it
		if now.Sub(limiter.lastRefill) > 10*time.Minute && limiter.tokens == limiter.maxTokens {
			toRemove = append(toRemove, connID)
		}
	}

	for _, connID := range toRemove {
		delete(pcrl.limiters, connID)
	}
}

// AdaptiveRateLimiter adjusts rate limits based on system load
type AdaptiveRateLimiter struct {
	baseLimiter    *RateLimiter
	mu             sync.RWMutex
	baseRate       int
	maxRate        int
	minRate        int
	window         time.Duration
	loadThreshold  float64
	adjustInterval time.Duration

	// Load tracking
	requestCount int64
	errorCount   int64
	lastAdjust   time.Time

	// Control
	adjustTicker *time.Ticker
	done         chan struct{}
}

// NewAdaptiveRateLimiter creates a new adaptive rate limiter
func NewAdaptiveRateLimiter(baseRate, minRate, maxRate int, window time.Duration, loadThreshold float64) *AdaptiveRateLimiter {
	arl := &AdaptiveRateLimiter{
		baseLimiter:    NewRateLimiter(baseRate, window),
		baseRate:       baseRate,
		maxRate:        maxRate,
		minRate:        minRate,
		window:         window,
		loadThreshold:  loadThreshold,
		adjustInterval: 30 * time.Second,
		lastAdjust:     time.Now(),
		done:           make(chan struct{}),
	}

	// Start adjustment routine
	arl.adjustTicker = time.NewTicker(arl.adjustInterval)
	go arl.adjustRate()

	return arl
}

// Allow checks if a request is allowed
func (arl *AdaptiveRateLimiter) Allow() bool {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	arl.requestCount++
	return arl.baseLimiter.Allow()
}

// ReportError reports an error (affects rate adjustment)
func (arl *AdaptiveRateLimiter) ReportError() {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	arl.errorCount++
}

// Close closes the adaptive rate limiter
func (arl *AdaptiveRateLimiter) Close() {
	close(arl.done)
	if arl.adjustTicker != nil {
		arl.adjustTicker.Stop()
	}
}

// GetCurrentRate returns the current rate limit
func (arl *AdaptiveRateLimiter) GetCurrentRate() int {
	arl.mu.RLock()
	defer arl.mu.RUnlock()

	return arl.baseLimiter.maxTokens
}

// Private methods

func (arl *AdaptiveRateLimiter) adjustRate() {
	for {
		select {
		case <-arl.done:
			return
		case <-arl.adjustTicker.C:
			arl.performAdjustment()
		}
	}
}

func (arl *AdaptiveRateLimiter) performAdjustment() {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	// Calculate error rate
	var errorRate float64
	if arl.requestCount > 0 {
		errorRate = float64(arl.errorCount) / float64(arl.requestCount)
	}

	currentRate := arl.baseLimiter.maxTokens
	newRate := currentRate

	// Adjust rate based on error rate
	if errorRate > arl.loadThreshold {
		// High error rate, decrease rate
		newRate = int(float64(currentRate) * 0.8)
		if newRate < arl.minRate {
			newRate = arl.minRate
		}
	} else if errorRate < arl.loadThreshold/2 {
		// Low error rate, increase rate
		newRate = int(float64(currentRate) * 1.2)
		if newRate > arl.maxRate {
			newRate = arl.maxRate
		}
	}

	// Apply new rate if changed
	if newRate != currentRate {
		arl.baseLimiter.SetRate(newRate, arl.window)
	}

	// Reset counters
	arl.requestCount = 0
	arl.errorCount = 0
	arl.lastAdjust = time.Now()
}
