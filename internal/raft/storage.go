package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"streaming-engine/pkg/types"
)

// FileStorage implements Storage interface using file system
type FileStorage struct {
	dir string
	mu  sync.RWMutex

	// Cached state
	currentTerm int64
	votedFor    *types.NodeID
	log         []*LogEntry
	snapshot    *Snapshot

	// File paths
	statePath    string
	logPath      string
	snapshotPath string
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	storage := &FileStorage{
		dir:          dir,
		statePath:    filepath.Join(dir, "state.json"),
		logPath:      filepath.Join(dir, "log.json"),
		snapshotPath: filepath.Join(dir, "snapshot.json"),
	}

	// Load existing state
	if err := storage.load(); err != nil {
		return nil, fmt.Errorf("failed to load storage: %w", err)
	}

	return storage, nil
}

// PersistentState represents the persistent state stored in files
type persistentState struct {
	CurrentTerm int64         `json:"current_term"`
	VotedFor    *types.NodeID `json:"voted_for"`
}

// GetCurrentTerm returns the current term
func (fs *FileStorage) GetCurrentTerm() (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.currentTerm, nil
}

// SetCurrentTerm sets the current term
func (fs *FileStorage) SetCurrentTerm(term int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.currentTerm = term
	return fs.saveState()
}

// GetVotedFor returns the node we voted for
func (fs *FileStorage) GetVotedFor() (*types.NodeID, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.votedFor, nil
}

// SetVotedFor sets the node we voted for
func (fs *FileStorage) SetVotedFor(nodeID *types.NodeID) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.votedFor = nodeID
	return fs.saveState()
}

// GetLogEntry returns a specific log entry
func (fs *FileStorage) GetLogEntry(index int64) (*LogEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for _, entry := range fs.log {
		if entry.Index == index {
			return entry, nil
		}
	}

	return nil, fmt.Errorf("log entry not found: %d", index)
}

// GetLogEntries returns log entries in the specified range
func (fs *FileStorage) GetLogEntries(start, end int64) ([]*LogEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var entries []*LogEntry
	for _, entry := range fs.log {
		if entry.Index >= start && entry.Index < end {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// AppendLogEntries appends log entries
func (fs *FileStorage) AppendLogEntries(entries []*LogEntry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.log = append(fs.log, entries...)
	return fs.saveLog()
}

// DeleteLogEntries deletes log entries from the specified index
func (fs *FileStorage) DeleteLogEntries(start int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var newLog []*LogEntry
	for _, entry := range fs.log {
		if entry.Index < start {
			newLog = append(newLog, entry)
		}
	}

	fs.log = newLog
	return fs.saveLog()
}

// GetLastLogIndex returns the index of the last log entry
func (fs *FileStorage) GetLastLogIndex() (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if len(fs.log) == 0 {
		if fs.snapshot != nil {
			return fs.snapshot.Index, nil
		}
		return 0, nil
	}

	return fs.log[len(fs.log)-1].Index, nil
}

// GetLastLogTerm returns the term of the last log entry
func (fs *FileStorage) GetLastLogTerm() (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if len(fs.log) == 0 {
		if fs.snapshot != nil {
			return fs.snapshot.Term, nil
		}
		return 0, nil
	}

	return fs.log[len(fs.log)-1].Term, nil
}

// GetFirstLogIndex returns the index of the first log entry
func (fs *FileStorage) GetFirstLogIndex() (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if len(fs.log) == 0 {
		if fs.snapshot != nil {
			return fs.snapshot.Index + 1, nil
		}
		return 1, nil
	}

	return fs.log[0].Index, nil
}

// GetSnapshot returns the current snapshot
func (fs *FileStorage) GetSnapshot() (*Snapshot, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.snapshot, nil
}

// SetSnapshot sets the current snapshot
func (fs *FileStorage) SetSnapshot(snapshot *Snapshot) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.snapshot = snapshot
	return fs.saveSnapshot()
}

// Sync ensures all data is persisted
func (fs *FileStorage) Sync() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.saveState(); err != nil {
		return err
	}

	if err := fs.saveLog(); err != nil {
		return err
	}

	if fs.snapshot != nil {
		if err := fs.saveSnapshot(); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the storage
func (fs *FileStorage) Close() error {
	return fs.Sync()
}

// Private methods

func (fs *FileStorage) load() error {
	// Load state
	if err := fs.loadState(); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Load log
	if err := fs.loadLog(); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Load snapshot
	if err := fs.loadSnapshot(); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (fs *FileStorage) loadState() error {
	data, err := os.ReadFile(fs.statePath)
	if err != nil {
		return err
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	fs.currentTerm = state.CurrentTerm
	fs.votedFor = state.VotedFor

	return nil
}

func (fs *FileStorage) saveState() error {
	state := persistentState{
		CurrentTerm: fs.currentTerm,
		VotedFor:    fs.votedFor,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	return fs.writeFile(fs.statePath, data)
}

func (fs *FileStorage) loadLog() error {
	data, err := os.ReadFile(fs.logPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &fs.log)
}

func (fs *FileStorage) saveLog() error {
	data, err := json.MarshalIndent(fs.log, "", "  ")
	if err != nil {
		return err
	}

	return fs.writeFile(fs.logPath, data)
}

func (fs *FileStorage) loadSnapshot() error {
	data, err := os.ReadFile(fs.snapshotPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &fs.snapshot)
}

func (fs *FileStorage) saveSnapshot() error {
	if fs.snapshot == nil {
		return nil
	}

	data, err := json.MarshalIndent(fs.snapshot, "", "  ")
	if err != nil {
		return err
	}

	return fs.writeFile(fs.snapshotPath, data)
}

func (fs *FileStorage) writeFile(path string, data []byte) error {
	// Write to temporary file first
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	// Atomic rename
	return os.Rename(tempPath, path)
}

// MemoryStorage implements Storage interface using memory
type MemoryStorage struct {
	mu sync.RWMutex

	currentTerm int64
	votedFor    *types.NodeID
	log         []*LogEntry
	snapshot    *Snapshot
}

// NewMemoryStorage creates a new memory-based storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		log: make([]*LogEntry, 0),
	}
}

// GetCurrentTerm returns the current term
func (ms *MemoryStorage) GetCurrentTerm() (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.currentTerm, nil
}

// SetCurrentTerm sets the current term
func (ms *MemoryStorage) SetCurrentTerm(term int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.currentTerm = term
	return nil
}

// GetVotedFor returns the node we voted for
func (ms *MemoryStorage) GetVotedFor() (*types.NodeID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.votedFor, nil
}

// SetVotedFor sets the node we voted for
func (ms *MemoryStorage) SetVotedFor(nodeID *types.NodeID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.votedFor = nodeID
	return nil
}

// GetLogEntry returns a specific log entry
func (ms *MemoryStorage) GetLogEntry(index int64) (*LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for _, entry := range ms.log {
		if entry.Index == index {
			return entry, nil
		}
	}

	return nil, fmt.Errorf("log entry not found: %d", index)
}

// GetLogEntries returns log entries in the specified range
func (ms *MemoryStorage) GetLogEntries(start, end int64) ([]*LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	var entries []*LogEntry
	for _, entry := range ms.log {
		if entry.Index >= start && entry.Index < end {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// AppendLogEntries appends log entries
func (ms *MemoryStorage) AppendLogEntries(entries []*LogEntry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.log = append(ms.log, entries...)
	return nil
}

// DeleteLogEntries deletes log entries from the specified index
func (ms *MemoryStorage) DeleteLogEntries(start int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var newLog []*LogEntry
	for _, entry := range ms.log {
		if entry.Index < start {
			newLog = append(newLog, entry)
		}
	}

	ms.log = newLog
	return nil
}

// GetLastLogIndex returns the index of the last log entry
func (ms *MemoryStorage) GetLastLogIndex() (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.log) == 0 {
		if ms.snapshot != nil {
			return ms.snapshot.Index, nil
		}
		return 0, nil
	}

	return ms.log[len(ms.log)-1].Index, nil
}

// GetLastLogTerm returns the term of the last log entry
func (ms *MemoryStorage) GetLastLogTerm() (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.log) == 0 {
		if ms.snapshot != nil {
			return ms.snapshot.Term, nil
		}
		return 0, nil
	}

	return ms.log[len(ms.log)-1].Term, nil
}

// GetFirstLogIndex returns the index of the first log entry
func (ms *MemoryStorage) GetFirstLogIndex() (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.log) == 0 {
		if ms.snapshot != nil {
			return ms.snapshot.Index + 1, nil
		}
		return 1, nil
	}

	return ms.log[0].Index, nil
}

// GetSnapshot returns the current snapshot
func (ms *MemoryStorage) GetSnapshot() (*Snapshot, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.snapshot, nil
}

// SetSnapshot sets the current snapshot
func (ms *MemoryStorage) SetSnapshot(snapshot *Snapshot) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.snapshot = snapshot
	return nil
}

// Sync ensures all data is persisted (no-op for memory storage)
func (ms *MemoryStorage) Sync() error {
	return nil
}

// Close closes the storage (no-op for memory storage)
func (ms *MemoryStorage) Close() error {
	return nil
}
