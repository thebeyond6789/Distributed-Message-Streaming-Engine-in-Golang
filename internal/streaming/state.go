package streaming

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// MemoryStateStore implements StateStore using in-memory storage
type MemoryStateStore struct {
	data map[string]interface{}
	mu   sync.RWMutex
}

// NewMemoryStateStore creates a new memory state store
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		data: make(map[string]interface{}),
	}
}

// Get retrieves a value by key
func (mss *MemoryStateStore) Get(key string) (interface{}, error) {
	mss.mu.RLock()
	defer mss.mu.RUnlock()

	value, exists := mss.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	return value, nil
}

// Put stores a key-value pair
func (mss *MemoryStateStore) Put(key string, value interface{}) error {
	mss.mu.Lock()
	defer mss.mu.Unlock()

	mss.data[key] = value
	return nil
}

// Delete removes a key-value pair
func (mss *MemoryStateStore) Delete(key string) error {
	mss.mu.Lock()
	defer mss.mu.Unlock()

	delete(mss.data, key)
	return nil
}

// Range returns all key-value pairs in the given range
func (mss *MemoryStateStore) Range(startKey, endKey string) (map[string]interface{}, error) {
	mss.mu.RLock()
	defer mss.mu.RUnlock()

	result := make(map[string]interface{})

	for key, value := range mss.data {
		if key >= startKey && key <= endKey {
			result[key] = value
		}
	}

	return result, nil
}

// Clear removes all key-value pairs
func (mss *MemoryStateStore) Clear() error {
	mss.mu.Lock()
	defer mss.mu.Unlock()

	mss.data = make(map[string]interface{})
	return nil
}

// Size returns the number of key-value pairs
func (mss *MemoryStateStore) Size() int64 {
	mss.mu.RLock()
	defer mss.mu.RUnlock()

	return int64(len(mss.data))
}

// Keys returns all keys
func (mss *MemoryStateStore) Keys() []string {
	mss.mu.RLock()
	defer mss.mu.RUnlock()

	keys := make([]string, 0, len(mss.data))
	for key := range mss.data {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

// Close closes the state store
func (mss *MemoryStateStore) Close() error {
	return nil
}

// FileStateStore implements StateStore using file-based storage
type FileStateStore struct {
	path string
	data map[string]interface{}
	mu   sync.RWMutex
}

// NewFileStateStore creates a new file-based state store
func NewFileStateStore(path string) (*FileStateStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	store := &FileStateStore{
		path: path,
		data: make(map[string]interface{}),
	}

	// Load existing data
	if err := store.load(); err != nil {
		return nil, fmt.Errorf("failed to load data: %w", err)
	}

	return store, nil
}

// Get retrieves a value by key
func (fss *FileStateStore) Get(key string) (interface{}, error) {
	fss.mu.RLock()
	defer fss.mu.RUnlock()

	value, exists := fss.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	return value, nil
}

// Put stores a key-value pair
func (fss *FileStateStore) Put(key string, value interface{}) error {
	fss.mu.Lock()
	defer fss.mu.Unlock()

	fss.data[key] = value
	return fss.save()
}

// Delete removes a key-value pair
func (fss *FileStateStore) Delete(key string) error {
	fss.mu.Lock()
	defer fss.mu.Unlock()

	delete(fss.data, key)
	return fss.save()
}

// Range returns all key-value pairs in the given range
func (fss *FileStateStore) Range(startKey, endKey string) (map[string]interface{}, error) {
	fss.mu.RLock()
	defer fss.mu.RUnlock()

	result := make(map[string]interface{})

	for key, value := range fss.data {
		if key >= startKey && key <= endKey {
			result[key] = value
		}
	}

	return result, nil
}

// Clear removes all key-value pairs
func (fss *FileStateStore) Clear() error {
	fss.mu.Lock()
	defer fss.mu.Unlock()

	fss.data = make(map[string]interface{})
	return fss.save()
}

// Size returns the number of key-value pairs
func (fss *FileStateStore) Size() int64 {
	fss.mu.RLock()
	defer fss.mu.RUnlock()

	return int64(len(fss.data))
}

// Keys returns all keys
func (fss *FileStateStore) Keys() []string {
	fss.mu.RLock()
	defer fss.mu.RUnlock()

	keys := make([]string, 0, len(fss.data))
	for key := range fss.data {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

// Close closes the state store
func (fss *FileStateStore) Close() error {
	return fss.save()
}

func (fss *FileStateStore) load() error {
	if _, err := os.Stat(fss.path); os.IsNotExist(err) {
		return nil // File doesn't exist yet
	}

	data, err := os.ReadFile(fss.path)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, &fss.data)
}

func (fss *FileStateStore) save() error {
	data, err := json.Marshal(fss.data)
	if err != nil {
		return err
	}

	return os.WriteFile(fss.path, data, 0644)
}

// MemoryWindowStore implements WindowStore using in-memory storage
type MemoryWindowStore struct {
	windows map[string]map[string]interface{} // windowKey -> key -> value
	mu      sync.RWMutex
}

// NewMemoryWindowStore creates a new memory window store
func NewMemoryWindowStore() *MemoryWindowStore {
	return &MemoryWindowStore{
		windows: make(map[string]map[string]interface{}),
	}
}

// Put stores a value for a key in a specific window
func (mws *MemoryWindowStore) Put(key string, value interface{}, window *Window) error {
	mws.mu.Lock()
	defer mws.mu.Unlock()

	windowKey := mws.windowKey(window)

	if _, exists := mws.windows[windowKey]; !exists {
		mws.windows[windowKey] = make(map[string]interface{})
	}

	mws.windows[windowKey][key] = value
	return nil
}

// Get retrieves a value for a key from a specific window
func (mws *MemoryWindowStore) Get(key string, window *Window) (interface{}, error) {
	mws.mu.RLock()
	defer mws.mu.RUnlock()

	windowKey := mws.windowKey(window)

	windowData, exists := mws.windows[windowKey]
	if !exists {
		return nil, fmt.Errorf("window not found")
	}

	value, exists := windowData[key]
	if !exists {
		return nil, fmt.Errorf("key not found in window")
	}

	return value, nil
}

// GetRange retrieves all values for a key across a time range
func (mws *MemoryWindowStore) GetRange(key string, from, to time.Time) (map[*Window]interface{}, error) {
	mws.mu.RLock()
	defer mws.mu.RUnlock()

	result := make(map[*Window]interface{})

	for windowKey, windowData := range mws.windows {
		window := mws.parseWindowKey(windowKey)
		if window != nil &&
			!window.Start.Before(from) &&
			!window.End.After(to) {
			if value, exists := windowData[key]; exists {
				result[window] = value
			}
		}
	}

	return result, nil
}

// DeleteExpired removes windows that have expired
func (mws *MemoryWindowStore) DeleteExpired(retentionTime time.Duration) error {
	mws.mu.Lock()
	defer mws.mu.Unlock()

	cutoff := time.Now().Add(-retentionTime)

	for windowKey := range mws.windows {
		window := mws.parseWindowKey(windowKey)
		if window != nil && window.End.Before(cutoff) {
			delete(mws.windows, windowKey)
		}
	}

	return nil
}

// Size returns the total number of windows
func (mws *MemoryWindowStore) Size() int64 {
	mws.mu.RLock()
	defer mws.mu.RUnlock()

	return int64(len(mws.windows))
}

// Windows returns all windows
func (mws *MemoryWindowStore) Windows() []*Window {
	mws.mu.RLock()
	defer mws.mu.RUnlock()

	var windows []*Window
	for windowKey := range mws.windows {
		window := mws.parseWindowKey(windowKey)
		if window != nil {
			windows = append(windows, window)
		}
	}

	return windows
}

// Close closes the window store
func (mws *MemoryWindowStore) Close() error {
	return nil
}

func (mws *MemoryWindowStore) windowKey(window *Window) string {
	return fmt.Sprintf("%s:%d:%d",
		window.Type,
		window.Start.UnixNano(),
		window.End.UnixNano())
}

func (mws *MemoryWindowStore) parseWindowKey(windowKey string) *Window {
	// This is a simplified parser - in practice, you'd want more robust parsing
	var windowType WindowType
	var start, end int64

	n, err := fmt.Sscanf(windowKey, "%s:%d:%d", &windowType, &start, &end)
	if err != nil || n != 3 {
		return nil
	}

	return &Window{
		Type:  windowType,
		Start: time.Unix(0, start),
		End:   time.Unix(0, end),
	}
}

// MemorySessionStore implements SessionStore using in-memory storage
type MemorySessionStore struct {
	sessions map[string]map[string]*SessionData // key -> sessionID -> data
	mu       sync.RWMutex
}

// SessionData represents session data
type SessionData struct {
	Value        interface{} `json:"value"`
	SessionStart time.Time   `json:"session_start"`
	SessionEnd   time.Time   `json:"session_end"`
	LastUpdate   time.Time   `json:"last_update"`
}

// NewMemorySessionStore creates a new memory session store
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		sessions: make(map[string]map[string]*SessionData),
	}
}

// Put stores a value for a key in a session
func (mss *MemorySessionStore) Put(key string, value interface{}, sessionStart, sessionEnd time.Time) error {
	mss.mu.Lock()
	defer mss.mu.Unlock()

	sessionID := fmt.Sprintf("%d-%d", sessionStart.UnixNano(), sessionEnd.UnixNano())

	if _, exists := mss.sessions[key]; !exists {
		mss.sessions[key] = make(map[string]*SessionData)
	}

	mss.sessions[key][sessionID] = &SessionData{
		Value:        value,
		SessionStart: sessionStart,
		SessionEnd:   sessionEnd,
		LastUpdate:   time.Now(),
	}

	return nil
}

// Get retrieves a value for a key from a session
func (mss *MemorySessionStore) Get(key string, sessionStart, sessionEnd time.Time) (interface{}, error) {
	mss.mu.RLock()
	defer mss.mu.RUnlock()

	sessionID := fmt.Sprintf("%d-%d", sessionStart.UnixNano(), sessionEnd.UnixNano())

	keySessions, exists := mss.sessions[key]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	sessionData, exists := keySessions[sessionID]
	if !exists {
		return nil, fmt.Errorf("session not found")
	}

	return sessionData.Value, nil
}

// GetSessions retrieves all sessions for a key
func (mss *MemorySessionStore) GetSessions(key string) (map[string]interface{}, error) {
	mss.mu.RLock()
	defer mss.mu.RUnlock()

	keySessions, exists := mss.sessions[key]
	if !exists {
		return make(map[string]interface{}), nil
	}

	result := make(map[string]interface{})
	for sessionID, sessionData := range keySessions {
		result[sessionID] = sessionData.Value
	}

	return result, nil
}

// MergeSessions merges overlapping sessions for a key
func (mss *MemorySessionStore) MergeSessions(key string, sessionGap time.Duration) error {
	mss.mu.Lock()
	defer mss.mu.Unlock()

	keySessions, exists := mss.sessions[key]
	if !exists {
		return nil
	}

	// Convert to slice for easier processing
	var sessionList []*SessionData
	for _, sessionData := range keySessions {
		sessionList = append(sessionList, sessionData)
	}

	// Sort by session start time
	sort.Slice(sessionList, func(i, j int) bool {
		return sessionList[i].SessionStart.Before(sessionList[j].SessionStart)
	})

	// Merge overlapping sessions
	var merged []*SessionData
	for _, session := range sessionList {
		if len(merged) == 0 {
			merged = append(merged, session)
			continue
		}

		last := merged[len(merged)-1]
		gap := session.SessionStart.Sub(last.SessionEnd)

		if gap <= sessionGap {
			// Merge sessions
			if session.SessionEnd.After(last.SessionEnd) {
				last.SessionEnd = session.SessionEnd
			}
			if session.LastUpdate.After(last.LastUpdate) {
				last.LastUpdate = session.LastUpdate
				last.Value = session.Value // Use latest value
			}
		} else {
			merged = append(merged, session)
		}
	}

	// Replace sessions with merged ones
	mss.sessions[key] = make(map[string]*SessionData)
	for _, session := range merged {
		sessionID := fmt.Sprintf("%d-%d", session.SessionStart.UnixNano(), session.SessionEnd.UnixNano())
		mss.sessions[key][sessionID] = session
	}

	return nil
}

// DeleteExpired removes expired sessions
func (mss *MemorySessionStore) DeleteExpired(retentionTime time.Duration) error {
	mss.mu.Lock()
	defer mss.mu.Unlock()

	cutoff := time.Now().Add(-retentionTime)

	for key, keySessions := range mss.sessions {
		for sessionID, sessionData := range keySessions {
			if sessionData.LastUpdate.Before(cutoff) {
				delete(keySessions, sessionID)
			}
		}

		if len(keySessions) == 0 {
			delete(mss.sessions, key)
		}
	}

	return nil
}

// Close closes the session store
func (mss *MemorySessionStore) Close() error {
	return nil
}

// CachingStateStore wraps another StateStore with caching
type CachingStateStore struct {
	underlying StateStore
	cache      map[string]interface{}
	maxSize    int
	mu         sync.RWMutex
}

// NewCachingStateStore creates a new caching state store
func NewCachingStateStore(underlying StateStore, maxSize int) *CachingStateStore {
	return &CachingStateStore{
		underlying: underlying,
		cache:      make(map[string]interface{}),
		maxSize:    maxSize,
	}
}

// Get retrieves a value by key (with caching)
func (css *CachingStateStore) Get(key string) (interface{}, error) {
	css.mu.RLock()
	if value, exists := css.cache[key]; exists {
		css.mu.RUnlock()
		return value, nil
	}
	css.mu.RUnlock()

	// Cache miss - get from underlying store
	value, err := css.underlying.Get(key)
	if err != nil {
		return nil, err
	}

	// Add to cache
	css.mu.Lock()
	if len(css.cache) < css.maxSize {
		css.cache[key] = value
	}
	css.mu.Unlock()

	return value, nil
}

// Put stores a key-value pair (updates cache)
func (css *CachingStateStore) Put(key string, value interface{}) error {
	if err := css.underlying.Put(key, value); err != nil {
		return err
	}

	css.mu.Lock()
	css.cache[key] = value
	css.mu.Unlock()

	return nil
}

// Delete removes a key-value pair (from cache too)
func (css *CachingStateStore) Delete(key string) error {
	if err := css.underlying.Delete(key); err != nil {
		return err
	}

	css.mu.Lock()
	delete(css.cache, key)
	css.mu.Unlock()

	return nil
}

// Range delegates to underlying store
func (css *CachingStateStore) Range(startKey, endKey string) (map[string]interface{}, error) {
	return css.underlying.Range(startKey, endKey)
}

// Clear delegates to underlying store and clears cache
func (css *CachingStateStore) Clear() error {
	if err := css.underlying.Clear(); err != nil {
		return err
	}

	css.mu.Lock()
	css.cache = make(map[string]interface{})
	css.mu.Unlock()

	return nil
}

// Size delegates to underlying store
func (css *CachingStateStore) Size() int64 {
	return css.underlying.Size()
}

// Keys delegates to underlying store
func (css *CachingStateStore) Keys() []string {
	return css.underlying.Keys()
}

// Close delegates to underlying store
func (css *CachingStateStore) Close() error {
	return css.underlying.Close()
}
