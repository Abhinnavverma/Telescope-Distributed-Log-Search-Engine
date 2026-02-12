package storage

import (
	"strings"
	"sync"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
)

type MemTable struct {
	mu          sync.RWMutex
	logs        []*proto.LogEntry
	index       map[string][]*proto.LogEntry
	lastIndexed int

	maxOffset int64

	notifyChan chan struct{}
	closeChan  chan struct{}
}

func NewMemTable() *MemTable {
	m := &MemTable{
		logs:  make([]*proto.LogEntry, 0, 1000),
		index: make(map[string][]*proto.LogEntry),

		// Initialize to -1 (meaning "No Kafka data seen yet")
		maxOffset: -1,

		notifyChan: make(chan struct{}, 1),
		closeChan:  make(chan struct{}),
	}
	go m.runAsyncIndexer()
	return m
}

func (m *MemTable) Add(entry *proto.LogEntry) {
	m.mu.Lock()
	m.logs = append(m.logs, entry)
	m.mu.Unlock()

	select {
	case m.notifyChan <- struct{}{}:
	default:
	}
}

// --- NEW: UpdateOffset ---
func (m *MemTable) UpdateOffset(offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offset > m.maxOffset {
		m.maxOffset = offset
	}
}

// --- UPDATED: Flush returns 3 values now! ---
func (m *MemTable) Flush() ([]*proto.LogEntry, map[string][]string, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.logs) == 0 {
		return nil, nil, -1
	}

	// 1. Force Indexing if Async is behind
	if m.lastIndexed < len(m.logs) {
		unindexedLogs := m.logs[m.lastIndexed:]
		for _, log := range unindexedLogs {
			m.addToIndexLocked(log)
		}
		m.lastIndexed = len(m.logs)
	}

	// 2. Capture Data
	oldLogs := m.logs
	currentOffset := m.maxOffset

	// 3. Transform Index (Pointers -> IDs)
	dbIndex := make(map[string][]string)
	for word, logPtrs := range m.index {
		ids := make([]string, len(logPtrs))
		for i, ptr := range logPtrs {
			ids[i] = ptr.Id
		}
		dbIndex[word] = ids
	}

	// 4. Reset Everything
	m.logs = make([]*proto.LogEntry, 0, 1000)
	m.index = make(map[string][]*proto.LogEntry)
	m.lastIndexed = 0
	m.maxOffset = -1

	return oldLogs, dbIndex, currentOffset
}

// Including them here for completeness:

func (m *MemTable) addToIndexLocked(log *proto.LogEntry) {
	m.index[log.Service] = append(m.index[log.Service], log)
	m.index[log.Level] = append(m.index[log.Level], log)
	words := strings.Fields(log.Body)
	for _, w := range words {
		m.index[w] = append(m.index[w], log)
	}
}

func (m *MemTable) SearchRAM(query, service, level string, start, end int64) []*proto.LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var candidates []*proto.LogEntry

	// 1. SELECT CANDIDATES
	if query != "" {
		hits, found := m.index[query]
		if !found {
			return nil
		}
		candidates = hits
	} else {
		// Since MemTables are small (<64MB), a linear scan is negligible (nanoseconds).
		candidates = m.logs
	}

	var results []*proto.LogEntry

	// 2. APPLY FILTERS (The "Where" Clause)
	for _, log := range candidates {
		// A. Time Filter (Always check bounds)
		// Note: We use 0 to mean "No Limit"
		if start > 0 && log.Timestamp < start {
			continue
		}
		if end > 0 && log.Timestamp > end {
			continue
		}

		// B. Service Filter
		if service != "" && log.Service != service {
			continue
		}

		// C. Level Filter
		if level != "" && log.Level != level {
			continue
		}

		// If we survived all checks, it's a match!
		results = append(results, log)
	}

	return results
}

func (m *MemTable) runAsyncIndexer() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.closeChan:
			return
		case <-m.notifyChan:
			m.processBatch()
		case <-ticker.C:
			m.processBatch()
		}
	}
}

func (m *MemTable) processBatch() {
	m.mu.RLock()
	currentLen := len(m.logs)
	startIdx := m.lastIndexed
	if currentLen <= startIdx {
		m.mu.RUnlock()
		return
	}
	newBatch := m.logs[startIdx:currentLen]
	m.mu.RUnlock()

	miniIndex := make(map[string][]*proto.LogEntry)
	for _, log := range newBatch {
		miniIndex[log.Service] = append(miniIndex[log.Service], log)
		miniIndex[log.Level] = append(miniIndex[log.Level], log)
		words := strings.Fields(log.Body)
		for _, w := range words {
			miniIndex[w] = append(miniIndex[w], log)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastIndexed != startIdx {
		return
	}
	for key, entries := range miniIndex {
		m.index[key] = append(m.index[key], entries...)
	}
	m.lastIndexed = currentLen
}
