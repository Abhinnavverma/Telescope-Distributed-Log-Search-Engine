package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Adapter struct {
	pool *pgxpool.Pool
}

func NewAdapter(pool *pgxpool.Pool) *Adapter {
	return &Adapter{pool: pool}
}

// WriteBatch writes logs, the inverted index segment, and the checkpoint in a SINGLE transaction.
func (a *Adapter) WriteBatch(ctx context.Context, logs []*proto.LogEntry, index map[string][]string, groupID string, newOffset int64) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}

	// Generate a unique Segment ID (The "Flush ID")
	segmentID := uuid.NewString()

	// 1. Insert Logs
	// Schema Fix: timestamp -> created_at
	for _, log := range logs {
		ts := time.Unix(0, log.Timestamp)
		batch.Queue(`
			INSERT INTO logs (id, trace_id, service, level, body, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO NOTHING`,
			log.Id, log.TraceId, log.Service, log.Level, log.Body, ts,
		)
	}

	// 2. Insert Inverted Index Segment (LSM Style)
	// Schema Fix: table 'inverse_index' -> 'inverted_index'
	// Schema Fix: col 'word' -> 'keyword'
	// Schema Fix: col 'timestamp' -> 'created_at'
	for keyword, logIDs := range index {
		batch.Queue(`
			INSERT INTO inverted_index (segment_id, keyword, log_ids, created_at)
			VALUES ($1, $2, $3, NOW())
		`, segmentID, keyword, logIDs)
	}

	// 3. Update Checkpoint
	// Schema Fix: consumer_group_id -> reader_group
	// Logic Fix: We use the passed 'groupID'.
	// TODO: Currently we hardcode topic="telescope-logs" and partition=0.
	// In a real multi-partition setup, 'newOffset' should probably be a map[int]int64
	// or passed individually. For v1 Single Partition, this is acceptable.
	batch.Queue(`
		INSERT INTO kafka_checkpoints (reader_group, topic, partition_id, last_offset, updated_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (reader_group, topic, partition_id) 
		DO UPDATE SET last_offset = EXCLUDED.last_offset, updated_at = NOW()`,
		groupID, "telescope-logs", 0, newOffset,
	)

	// Execute the batch
	br := tx.SendBatch(ctx, batch)

	// CRITICAL: You must close the batch results to check for errors
	// In pgx, Close() returns the first error encountered.
	if err := br.Close(); err != nil {
		return fmt.Errorf("failed to execute batch: %w", err)
	}

	return tx.Commit(ctx)
}

func (a *Adapter) Search(ctx context.Context, query string, start, end int64) ([]*proto.LogEntry, error) {
	// Optimized SQL with CTE
	// Schema Fix: table/column names updated
	sql := `
		WITH matched_ids AS (
			SELECT unnest(log_ids) as log_id 
			FROM inverted_index 
			WHERE keyword = $1
		)
		SELECT l.id, l.trace_id, l.service, l.level, l.body, l.created_at
		FROM logs l
		JOIN matched_ids m ON l.id = m.log_id
		WHERE l.created_at >= $2 AND l.created_at <= $3
		ORDER BY l.created_at DESC
		LIMIT 100`

	startTime := time.Unix(0, start)
	endTime := time.Unix(0, end)

	rows, err := a.pool.Query(ctx, sql, query, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []*proto.LogEntry
	for rows.Next() {
		var l proto.LogEntry
		var ts time.Time
		// Scan matches the SELECT order
		if err := rows.Scan(&l.Id, &l.TraceId, &l.Service, &l.Level, &l.Body, &ts); err != nil {
			return nil, err
		}
		l.Timestamp = ts.UnixNano()
		results = append(results, &l)
	}
	return results, nil
}

func (a *Adapter) GetCheckpoint(ctx context.Context, groupID string, topic string, partition int) (int64, error) {
	var offset int64

	// We default to -1 if no checkpoint exists (meaning "start from beginning")
	err := a.pool.QueryRow(ctx, `
        SELECT last_offset 
        FROM kafka_checkpoints 
        WHERE reader_group = $1 AND topic = $2 AND partition_id = $3`,
		groupID, topic, partition,
	).Scan(&offset)

	if err != nil {
		if err == pgx.ErrNoRows {
			return -1, nil // No history found, start fresh
		}
		return 0, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return offset, nil
}
