package storage

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
	"github.com/oklog/ulid/v2"
)

type StorageService struct {
	memTable      *MemTable
	repo          LogRepository
	flushInterval time.Duration
	consumerGroup string // <--- NEW: We need to know who we are!
	entropy       io.Reader
	idLock        sync.Mutex
}

// Updated Constructor
func NewStorageService(repo LogRepository, groupID string) *StorageService {
	t := time.Now()
	seed := t.UnixNano()
	source := rand.NewSource(seed)

	// 2. Use Monotonic entropy to prevent collisions in the same millisecond
	entropy := ulid.Monotonic(rand.New(source), 0)

	s := &StorageService{
		memTable:      NewMemTable(),
		repo:          repo,
		flushInterval: 1 * time.Second,
		consumerGroup: groupID,
		entropy:       entropy, // Store the entropy source
	}
	go s.startFlusher()
	return s
}

func (s *StorageService) Push(ctx context.Context, logs []*proto.LogEntry, kafkaOffset int64) error {
	for _, l := range logs {
		// 🚨 OVERRIDE: We generate the ID.
		// We use the Log's timestamp so the ID matches the event time.
		// If the log has no timestamp, we default to Now().
		var ms uint64
		if l.Timestamp > 0 {
			// Convert Nanoseconds (Proto) -> Milliseconds (ULID)
			ms = uint64(l.Timestamp / 1e6)
		} else {
			ms = ulid.Now()
			l.Timestamp = int64(ms * 1e6) // Backfill timestamp if missing
		}

		l.Id = s.generateULID(ms)

		s.memTable.Add(l)
	}

	if kafkaOffset >= 0 {
		s.memTable.UpdateOffset(kafkaOffset)
	}

	return nil
}

func (s *StorageService) startFlusher() {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.Flush()
	}
}

func (s *StorageService) Flush() {
	// 1. Get Logs, Index, AND Offset (3 values)
	logs, dbIndex, offset := s.memTable.Flush()

	if len(logs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2. Write Batch using the REAL consumer group and REAL offset
	err := s.repo.WriteBatch(ctx, logs, dbIndex, s.consumerGroup, offset)

	if err != nil {
		log.Printf("🔥 ERROR flushing to DB: %v", err)
	} else {
		log.Printf("✅ Flushed %d logs to Postgres (Offset: %d)", len(logs), offset)
	}
}

func (s *StorageService) Search(ctx context.Context, query string, start, end int64) ([]*proto.LogEntry, error) {
	ramResults := s.memTable.SearchRAM(query, start, end)
	diskResults, err := s.repo.Search(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	return append(ramResults, diskResults...), nil
}

func (s *StorageService) generateULID(ms uint64) string {
	s.idLock.Lock()
	defer s.idLock.Unlock()

	// ulid.New returns an Error only if entropy fails,
	// but math/rand never fails, so we can ignore the error or panic.
	id, err := ulid.New(ms, s.entropy)
	if err != nil {
		// This should physically never happen with math/rand
		panic(err)
	}
	return id.String()
}

func (s *StorageService) GetLastOffset(ctx context.Context, topic string, partition int) (int64, error) {
	return s.repo.GetCheckpoint(ctx, s.consumerGroup, topic, partition)
}
