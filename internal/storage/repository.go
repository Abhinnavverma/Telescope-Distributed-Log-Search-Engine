package storage

import (
	"context"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
)

type LogRepository interface {
	// WriteBatch saves Logs AND the Inverted Index atomically.
	WriteBatch(ctx context.Context, logs []*proto.LogEntry, index map[string][]string, checkpointID string, newOffset int64) error

	// Search queries the database.
	Search(ctx context.Context, query string, start, end int64, limit int, service string, level string) ([]*proto.LogEntry, error)
	GetCheckpoint(ctx context.Context, groupID string, topic string, partition int) (int64, error)
	DeleteOldLogs(ctx context.Context, cutoff time.Time) (int64, error)
	CompactSegments(ctx context.Context, day time.Time) error
	OptimizeIndex(ctx context.Context) error
}
