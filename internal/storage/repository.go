package storage

import (
	"context"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
)

type LogRepository interface {
	// WriteBatch saves Logs AND the Inverted Index atomically.
	// index: map[Word] -> []LogID
	WriteBatch(ctx context.Context, logs []*proto.LogEntry, index map[string][]string, checkpointID string, newOffset int64) error

	// Search queries the database.
	Search(ctx context.Context, query string, start, end int64) ([]*proto.LogEntry, error)
	GetCheckpoint(ctx context.Context, groupID string, topic string, partition int) (int64, error)
}
