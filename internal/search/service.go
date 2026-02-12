package search_api

import (
	"context"
	"sort"
	"sync"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
)

type SearchService struct {
	clients []StorageClient
}

func NewSearchService(clients []StorageClient) *SearchService {
	return &SearchService{
		clients: clients,
	}
}

// CHANGED: Added start, end, limit
func (s *SearchService) Search(ctx context.Context, query string, start, end int64, limit int, service string, level string) ([]*proto.LogEntry, error) {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allLogs []*proto.LogEntry
	)

	// SCATTER: Launch a goroutine for every node
	for _, client := range s.clients {
		wg.Add(1)
		go func(c StorageClient) {
			defer wg.Done()

			logs, err := c.Search(ctx, query, start, end, int64(limit), service, level)
			if err != nil {
				// Log error internally, but allow partial results
				return
			}

			mu.Lock()
			allLogs = append(allLogs, logs...)
			mu.Unlock()
		}(client)
	}

	wg.Wait()

	// GATHER: Sort combined results by Timestamp (Descending)
	sort.Slice(allLogs, func(i, j int) bool {
		return allLogs[i].Timestamp > allLogs[j].Timestamp
	})

	// Global Limit: Cut off the excess
	if len(allLogs) > limit {
		allLogs = allLogs[:limit]
	}

	return allLogs, nil
}
