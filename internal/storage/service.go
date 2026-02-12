package storage

import (
	"context"
	"fmt"
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
	consumerGroup string
	entropy       io.Reader
	idLock        sync.Mutex
}

func NewStorageService(repo LogRepository, groupID string) *StorageService {
	t := time.Now()
	seed := t.UnixNano()
	source := rand.NewSource(seed)
	entropy := ulid.Monotonic(rand.New(source), 0)

	s := &StorageService{
		memTable:      NewMemTable(),
		repo:          repo,
		flushInterval: 1 * time.Second,
		consumerGroup: groupID,
		entropy:       entropy,
	}
	go s.startFlusher()
	return s
}

func (s *StorageService) Push(ctx context.Context, logs []*proto.LogEntry, kafkaOffset int64) error {
	for _, l := range logs {
		var ms uint64
		if l.Timestamp > 0 {
			ms = uint64(l.Timestamp / 1e6)
		} else {
			ms = ulid.Now()
			l.Timestamp = int64(ms * 1e6)
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
	logs, dbIndex, offset := s.memTable.Flush()
	if len(logs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := s.repo.WriteBatch(ctx, logs, dbIndex, s.consumerGroup, offset)
	if err != nil {
		log.Printf(" ERROR flushing to DB: %v", err)
	} else {
		log.Printf(" Flushed %d logs to Postgres (Offset: %d)", len(logs), offset)
	}
}

func (s *StorageService) Search(ctx context.Context, query string, start, end int64, limit int, service string, level string) ([]*proto.LogEntry, error) {
	fmt.Printf(" [3] STORAGE RECEIVED: Service='%s'\n", service)
	ramResults := s.memTable.SearchRAM(query, service, level, start, end)

	diskResults, err := s.repo.Search(ctx, query, start, end, limit, service, level)
	if err != nil {
		return nil, err
	}

	allResults := append(ramResults, diskResults...)

	if len(allResults) > limit {
		return allResults[:limit], nil
	}

	return allResults, nil
}

func (s *StorageService) generateULID(ms uint64) string {
	s.idLock.Lock()
	defer s.idLock.Unlock()
	id, err := ulid.New(ms, s.entropy)
	if err != nil {
		panic(err)
	}
	return id.String()
}

func (s *StorageService) GetLastOffset(ctx context.Context, topic string, partition int) (int64, error) {
	return s.repo.GetCheckpoint(ctx, s.consumerGroup, topic, partition)
}
