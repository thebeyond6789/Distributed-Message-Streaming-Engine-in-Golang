package streaming

import (
	"fmt"
	"sync"
	"time"
)

// WindowManager manages different types of windows
type WindowManager struct {
	mu              sync.RWMutex
	tumblingWindows map[string]*TumblingWindowSet
	slidingWindows  map[string]*SlidingWindowSet
	sessionWindows  map[string]*SessionWindowSet
	globalWindows   map[string]*GlobalWindowSet
	config          *WindowConfig
}

// WindowConfig represents window configuration
type WindowConfig struct {
	AllowedLateness    time.Duration                 `json:"allowed_lateness"`
	RetentionTime      time.Duration                 `json:"retention_time"`
	CleanupInterval    time.Duration                 `json:"cleanup_interval"`
	TimestampExtractor func(*StreamRecord) time.Time `json:"-"`
}

// DefaultWindowConfig returns a default window configuration
func DefaultWindowConfig() *WindowConfig {
	return &WindowConfig{
		AllowedLateness: 10 * time.Second,
		RetentionTime:   1 * time.Hour,
		CleanupInterval: 5 * time.Minute,
		TimestampExtractor: func(record *StreamRecord) time.Time {
			return record.Timestamp
		},
	}
}

// NewWindowManager creates a new window manager
func NewWindowManager(config *WindowConfig) *WindowManager {
	if config == nil {
		config = DefaultWindowConfig()
	}

	wm := &WindowManager{
		tumblingWindows: make(map[string]*TumblingWindowSet),
		slidingWindows:  make(map[string]*SlidingWindowSet),
		sessionWindows:  make(map[string]*SessionWindowSet),
		globalWindows:   make(map[string]*GlobalWindowSet),
		config:          config,
	}

	// Start cleanup routine
	go wm.cleanupRoutine()

	return wm
}

// CreateTumblingWindow creates a tumbling window
func (wm *WindowManager) CreateTumblingWindow(name string, duration time.Duration) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.tumblingWindows[name]; exists {
		return fmt.Errorf("tumbling window %s already exists", name)
	}

	wm.tumblingWindows[name] = NewTumblingWindowSet(duration, wm.config)
	return nil
}

// CreateSlidingWindow creates a sliding window
func (wm *WindowManager) CreateSlidingWindow(name string, duration, slideSize time.Duration) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.slidingWindows[name]; exists {
		return fmt.Errorf("sliding window %s already exists", name)
	}

	wm.slidingWindows[name] = NewSlidingWindowSet(duration, slideSize, wm.config)
	return nil
}

// CreateSessionWindow creates a session window
func (wm *WindowManager) CreateSessionWindow(name string, sessionGap time.Duration) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.sessionWindows[name]; exists {
		return fmt.Errorf("session window %s already exists", name)
	}

	wm.sessionWindows[name] = NewSessionWindowSet(sessionGap, wm.config)
	return nil
}

// CreateGlobalWindow creates a global window
func (wm *WindowManager) CreateGlobalWindow(name string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.globalWindows[name]; exists {
		return fmt.Errorf("global window %s already exists", name)
	}

	wm.globalWindows[name] = NewGlobalWindowSet(wm.config)
	return nil
}

// AssignWindows assigns a record to appropriate windows
func (wm *WindowManager) AssignWindows(windowName string, record *StreamRecord) ([]*Window, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	timestamp := wm.config.TimestampExtractor(record)

	// Check tumbling windows
	if tws, exists := wm.tumblingWindows[windowName]; exists {
		return tws.AssignWindows(timestamp), nil
	}

	// Check sliding windows
	if sws, exists := wm.slidingWindows[windowName]; exists {
		return sws.AssignWindows(timestamp), nil
	}

	// Check session windows
	if sesws, exists := wm.sessionWindows[windowName]; exists {
		return sesws.AssignWindows(record.Key, timestamp), nil
	}

	// Check global windows
	if gws, exists := wm.globalWindows[windowName]; exists {
		return gws.AssignWindows(), nil
	}

	return nil, fmt.Errorf("window %s not found", windowName)
}

// GetExpiredWindows returns expired windows for cleanup
func (wm *WindowManager) GetExpiredWindows(watermark time.Time) []*Window {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var expired []*Window

	// Check tumbling windows
	for _, tws := range wm.tumblingWindows {
		expired = append(expired, tws.GetExpiredWindows(watermark)...)
	}

	// Check sliding windows
	for _, sws := range wm.slidingWindows {
		expired = append(expired, sws.GetExpiredWindows(watermark)...)
	}

	// Check session windows
	for _, sesws := range wm.sessionWindows {
		expired = append(expired, sesws.GetExpiredWindows(watermark)...)
	}

	return expired
}

// Close closes the window manager
func (wm *WindowManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Clear all windows
	wm.tumblingWindows = make(map[string]*TumblingWindowSet)
	wm.slidingWindows = make(map[string]*SlidingWindowSet)
	wm.sessionWindows = make(map[string]*SessionWindowSet)
	wm.globalWindows = make(map[string]*GlobalWindowSet)

	return nil
}

func (wm *WindowManager) cleanupRoutine() {
	ticker := time.NewTicker(wm.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		watermark := time.Now().Add(-wm.config.AllowedLateness)
		wm.GetExpiredWindows(watermark) // This will trigger cleanup
	}
}

// TumblingWindowSet represents a set of tumbling windows
type TumblingWindowSet struct {
	duration time.Duration
	config   *WindowConfig
	mu       sync.RWMutex
	windows  map[int64]*Window // timestamp -> window
}

// NewTumblingWindowSet creates a new tumbling window set
func NewTumblingWindowSet(duration time.Duration, config *WindowConfig) *TumblingWindowSet {
	return &TumblingWindowSet{
		duration: duration,
		config:   config,
		windows:  make(map[int64]*Window),
	}
}

// AssignWindows assigns a timestamp to tumbling windows
func (tws *TumblingWindowSet) AssignWindows(timestamp time.Time) []*Window {
	tws.mu.Lock()
	defer tws.mu.Unlock()

	// Calculate window start
	windowStart := timestamp.Truncate(tws.duration)
	windowEnd := windowStart.Add(tws.duration)

	windowKey := windowStart.UnixNano()
	window, exists := tws.windows[windowKey]
	if !exists {
		window = &Window{
			Type:     WindowTypeTumbling,
			Start:    windowStart,
			End:      windowEnd,
			Duration: tws.duration,
		}
		tws.windows[windowKey] = window
	}

	return []*Window{window}
}

// GetExpiredWindows returns expired tumbling windows
func (tws *TumblingWindowSet) GetExpiredWindows(watermark time.Time) []*Window {
	tws.mu.Lock()
	defer tws.mu.Unlock()

	var expired []*Window
	var expiredKeys []int64

	for key, window := range tws.windows {
		if window.End.Add(tws.config.AllowedLateness).Before(watermark) {
			expired = append(expired, window)
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Remove expired windows
	for _, key := range expiredKeys {
		delete(tws.windows, key)
	}

	return expired
}

// SlidingWindowSet represents a set of sliding windows
type SlidingWindowSet struct {
	duration  time.Duration
	slideSize time.Duration
	config    *WindowConfig
	mu        sync.RWMutex
	windows   map[int64]*Window // timestamp -> window
}

// NewSlidingWindowSet creates a new sliding window set
func NewSlidingWindowSet(duration, slideSize time.Duration, config *WindowConfig) *SlidingWindowSet {
	return &SlidingWindowSet{
		duration:  duration,
		slideSize: slideSize,
		config:    config,
		windows:   make(map[int64]*Window),
	}
}

// AssignWindows assigns a timestamp to sliding windows
func (sws *SlidingWindowSet) AssignWindows(timestamp time.Time) []*Window {
	sws.mu.Lock()
	defer sws.mu.Unlock()

	var windows []*Window

	// Calculate all overlapping windows
	firstWindowStart := timestamp.Add(-sws.duration).Truncate(sws.slideSize)
	if firstWindowStart.Add(sws.duration).Before(timestamp) {
		firstWindowStart = firstWindowStart.Add(sws.slideSize)
	}

	for windowStart := firstWindowStart; windowStart.Add(sws.duration).After(timestamp); windowStart = windowStart.Add(sws.slideSize) {
		if windowStart.After(timestamp) {
			break
		}

		windowEnd := windowStart.Add(sws.duration)
		windowKey := windowStart.UnixNano()

		window, exists := sws.windows[windowKey]
		if !exists {
			window = &Window{
				Type:      WindowTypeSliding,
				Start:     windowStart,
				End:       windowEnd,
				Duration:  sws.duration,
				SlideSize: sws.slideSize,
			}
			sws.windows[windowKey] = window
		}

		windows = append(windows, window)
	}

	return windows
}

// GetExpiredWindows returns expired sliding windows
func (sws *SlidingWindowSet) GetExpiredWindows(watermark time.Time) []*Window {
	sws.mu.Lock()
	defer sws.mu.Unlock()

	var expired []*Window
	var expiredKeys []int64

	for key, window := range sws.windows {
		if window.End.Add(sws.config.AllowedLateness).Before(watermark) {
			expired = append(expired, window)
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Remove expired windows
	for _, key := range expiredKeys {
		delete(sws.windows, key)
	}

	return expired
}

// SessionWindowSet represents a set of session windows
type SessionWindowSet struct {
	sessionGap time.Duration
	config     *WindowConfig
	mu         sync.RWMutex
	sessions   map[string][]*SessionWindow // key -> sessions
}

// SessionWindow represents a session window
type SessionWindow struct {
	*Window
	Key        string
	LastUpdate time.Time
}

// NewSessionWindowSet creates a new session window set
func NewSessionWindowSet(sessionGap time.Duration, config *WindowConfig) *SessionWindowSet {
	return &SessionWindowSet{
		sessionGap: sessionGap,
		config:     config,
		sessions:   make(map[string][]*SessionWindow),
	}
}

// AssignWindows assigns a timestamp to session windows
func (sws *SessionWindowSet) AssignWindows(key string, timestamp time.Time) []*Window {
	sws.mu.Lock()
	defer sws.mu.Unlock()

	sessions := sws.sessions[key]

	// Find or create appropriate session
	var targetSession *SessionWindow
	var toMerge []*SessionWindow

	for i, session := range sessions {
		// Check if timestamp falls within session gap
		if timestamp.Sub(session.LastUpdate) <= sws.sessionGap ||
			session.LastUpdate.Sub(timestamp) <= sws.sessionGap {
			if targetSession == nil {
				targetSession = session
			} else {
				toMerge = append(toMerge, session)
				// Remove from slice
				sessions = append(sessions[:i], sessions[i+1:]...)
			}
		}
	}

	if targetSession == nil {
		// Create new session
		targetSession = &SessionWindow{
			Window: &Window{
				Type:       WindowTypeSession,
				Start:      timestamp,
				End:        timestamp,
				SessionGap: sws.sessionGap,
			},
			Key:        key,
			LastUpdate: timestamp,
		}
		sessions = append(sessions, targetSession)
	} else {
		// Extend session
		if timestamp.Before(targetSession.Start) {
			targetSession.Start = timestamp
		}
		if timestamp.After(targetSession.End) {
			targetSession.End = timestamp
		}
		targetSession.LastUpdate = timestamp

		// Merge sessions if needed
		for _, session := range toMerge {
			if session.Start.Before(targetSession.Start) {
				targetSession.Start = session.Start
			}
			if session.End.After(targetSession.End) {
				targetSession.End = session.End
			}
			if session.LastUpdate.After(targetSession.LastUpdate) {
				targetSession.LastUpdate = session.LastUpdate
			}
		}
	}

	sws.sessions[key] = sessions
	return []*Window{targetSession.Window}
}

// GetExpiredWindows returns expired session windows
func (sws *SessionWindowSet) GetExpiredWindows(watermark time.Time) []*Window {
	sws.mu.Lock()
	defer sws.mu.Unlock()

	var expired []*Window

	for key, sessions := range sws.sessions {
		var activeSessions []*SessionWindow

		for _, session := range sessions {
			if session.LastUpdate.Add(sws.sessionGap).Add(sws.config.AllowedLateness).Before(watermark) {
				expired = append(expired, session.Window)
			} else {
				activeSessions = append(activeSessions, session)
			}
		}

		if len(activeSessions) == 0 {
			delete(sws.sessions, key)
		} else {
			sws.sessions[key] = activeSessions
		}
	}

	return expired
}

// GlobalWindowSet represents a global window (no windowing)
type GlobalWindowSet struct {
	config *WindowConfig
	window *Window
	mu     sync.RWMutex
}

// NewGlobalWindowSet creates a new global window set
func NewGlobalWindowSet(config *WindowConfig) *GlobalWindowSet {
	return &GlobalWindowSet{
		config: config,
		window: &Window{
			Type:  WindowTypeGlobal,
			Start: time.Time{},           // Beginning of time
			End:   time.Unix(1<<63-1, 0), // End of time
		},
	}
}

// AssignWindows assigns to the global window
func (gws *GlobalWindowSet) AssignWindows() []*Window {
	gws.mu.RLock()
	defer gws.mu.RUnlock()
	return []*Window{gws.window}
}

// WindowAssigner is a utility for window assignment
type WindowAssigner struct {
	windowManager *WindowManager
}

// NewWindowAssigner creates a new window assigner
func NewWindowAssigner(windowManager *WindowManager) *WindowAssigner {
	return &WindowAssigner{
		windowManager: windowManager,
	}
}

// Assign assigns a record to windows
func (wa *WindowAssigner) Assign(windowName string, record *StreamRecord) ([]*Window, error) {
	return wa.windowManager.AssignWindows(windowName, record)
}

// WindowTrigger represents different window trigger policies
type WindowTrigger interface {
	ShouldFire(window *Window, record *StreamRecord, count int64) bool
	Reset()
}

// CountTrigger triggers based on element count
type CountTrigger struct {
	maxCount int64
}

// NewCountTrigger creates a new count trigger
func NewCountTrigger(maxCount int64) *CountTrigger {
	return &CountTrigger{maxCount: maxCount}
}

// ShouldFire checks if the trigger should fire
func (ct *CountTrigger) ShouldFire(window *Window, record *StreamRecord, count int64) bool {
	return count >= ct.maxCount
}

// Reset resets the trigger
func (ct *CountTrigger) Reset() {
	// No state to reset for count trigger
}

// TimeTrigger triggers based on processing time
type TimeTrigger struct {
	interval    time.Duration
	lastTrigger time.Time
}

// NewTimeTrigger creates a new time trigger
func NewTimeTrigger(interval time.Duration) *TimeTrigger {
	return &TimeTrigger{
		interval:    interval,
		lastTrigger: time.Now(),
	}
}

// ShouldFire checks if the trigger should fire
func (tt *TimeTrigger) ShouldFire(window *Window, record *StreamRecord, count int64) bool {
	now := time.Now()
	if now.Sub(tt.lastTrigger) >= tt.interval {
		tt.lastTrigger = now
		return true
	}
	return false
}

// Reset resets the trigger
func (tt *TimeTrigger) Reset() {
	tt.lastTrigger = time.Now()
}

// WatermarkTrigger triggers based on watermarks
type WatermarkTrigger struct {
	allowedLateness time.Duration
}

// NewWatermarkTrigger creates a new watermark trigger
func NewWatermarkTrigger(allowedLateness time.Duration) *WatermarkTrigger {
	return &WatermarkTrigger{allowedLateness: allowedLateness}
}

// ShouldFire checks if the trigger should fire
func (wt *WatermarkTrigger) ShouldFire(window *Window, record *StreamRecord, count int64) bool {
	watermark := time.Now() // In a real implementation, this would come from the watermark generator
	return watermark.After(window.End.Add(wt.allowedLateness))
}

// Reset resets the trigger
func (wt *WatermarkTrigger) Reset() {
	// No state to reset for watermark trigger
}
