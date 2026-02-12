package postgres

import (
	"context"
	"fmt"
	"log"
	"strings"
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

	for keyword, logIDs := range index {
		batch.Queue(`
			INSERT INTO inverted_index (segment_id, keyword, log_ids, created_at)
			VALUES ($1, $2, $3, NOW())
		`, segmentID, keyword, logIDs)
	}

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

func (a *Adapter) Search(ctx context.Context, query string, start, end int64, limit int, service string, level string) ([]*proto.LogEntry, error) {
	// 1. Dynamic Query Builder
	fmt.Printf("Adapter recieved the service %s", service)
	var queryBuilder strings.Builder
	args := []interface{}{}
	argId := 1

	// Scenario A: Full-Text Search (Use Inverted Index CTE)
	if query != "" {
		queryBuilder.WriteString(fmt.Sprintf(`
			WITH matched_ids AS (
				SELECT unnest(log_ids) as log_id 
				FROM inverted_index 
				WHERE keyword = $%d
			)
			SELECT l.id, l.trace_id, l.service, l.level, l.body, l.created_at
			FROM logs l
			JOIN matched_ids m ON l.id = m.log_id
			WHERE 1=1 `, argId))
		args = append(args, query)
		argId++
	} else {
		// Scenario B: Filter Only (Direct Table Scan - Much Faster)
		queryBuilder.WriteString(`
			SELECT id, trace_id, service, level, body, created_at
			FROM logs
			WHERE 1=1 `)
	}

	// 2. Apply Filters (Service, Level, Time)
	if service != "" {
		queryBuilder.WriteString(fmt.Sprintf(" AND service = $%d", argId))
		args = append(args, service)
		argId++
	}
	if level != "" {
		queryBuilder.WriteString(fmt.Sprintf(" AND level = $%d", argId))
		args = append(args, level)
		argId++
	}
	if start > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" AND created_at >= $%d", argId))
		args = append(args, time.Unix(0, start))
		argId++
	}
	if end > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" AND created_at <= $%d", argId))
		args = append(args, time.Unix(0, end))
		argId++
	}

	// 3. Sort & Limit (Safety First)
	if limit <= 0 {
		limit = 100
	} // Default safety limit

	queryBuilder.WriteString(fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", argId))
	args = append(args, limit)

	// 4. Execute
	rows, err := a.pool.Query(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("search query failed: %w", err)
	}
	defer rows.Close()

	// 5. Map Results
	var results []*proto.LogEntry
	for rows.Next() {
		var l proto.LogEntry
		var ts time.Time
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

// --- Janitor Logic ---

// DeleteOldLogs deletes any log entry older than the cutoff time.
// Returns the number of rows deleted.
func (a *Adapter) DeleteOldLogs(ctx context.Context, cutoff time.Time) (int64, error) {
	// 1. Delete from the main logs table
	// (If you set up ON DELETE CASCADE foreign keys, this cleans the index too!)
	// FIX 1: Use correct column 'created_at' instead of 'timestamp'
	queryLogs := `DELETE FROM logs WHERE created_at < $1`

	tagLogs, err := a.pool.Exec(ctx, queryLogs, cutoff)
	if err != nil {
		return 0, fmt.Errorf("janitor failed to delete logs: %w", err)
	}

	// FIX 2: Also clean up the Inverted Index to prevent infinite disk growth
	queryIndex := `DELETE FROM inverted_index WHERE created_at < $1`
	tagIndex, err := a.pool.Exec(ctx, queryIndex, cutoff)
	if err != nil {
		// Log error but don't fail the whole job, as logs are already gone
		log.Printf("⚠️ Janitor warning: failed to clean index: %v", err)
	} else {
		log.Printf("🧹 Janitor also removed %d expired index entries.", tagIndex.RowsAffected())
	}

	return tagLogs.RowsAffected(), nil
}

// --- Merger Logic (The LSM Compaction) ---

// CompactSegments merges many small index entries into one big entry for a specific day.
// This reduces "Read Amplification" (checking 1000 rows for one keyword).
func (a *Adapter) CompactSegments(ctx context.Context, day time.Time) error {
	dateStr := day.Format("2006-01-02")
	// We create a special Segment ID for the merged data so we don't merge it again.
	mergedSegmentID := fmt.Sprintf("MERGED_%s", day.Format("20060102"))

	log.Printf("🏗️ Compacting segments for date: %s", dateStr)

	// Transaction ensures we don't lose data if we crash halfway
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// A. The "Merge" Step:
	// 1. Find all entries for that day that are NOT already merged.
	// 2. Group them by Keyword.
	// 3. Combine their LogIDs into one big array.
	// 4. Insert the new "Mega Row".
	queryMerge := `
        INSERT INTO inverted_index (segment_id, keyword, log_ids, created_at)
        SELECT 
            $1,                     -- The new MERGED_ID
            keyword, 
            array_agg(DISTINCT id), -- Combine & Deduplicate IDs
            NOW()
        FROM (
            SELECT keyword, unnest(log_ids) as id
            FROM inverted_index
            WHERE created_at::date = $2::date
              AND segment_id NOT LIKE 'MERGED_%' -- Only touch raw segments
        ) unpacked
        GROUP BY keyword
        ON CONFLICT (segment_id, keyword) DO NOTHING;
    `

	tag, err := tx.Exec(ctx, queryMerge, mergedSegmentID, dateStr)
	if err != nil {
		return fmt.Errorf("failed to merge index: %w", err)
	}
	log.Printf("✨ Created %d merged index entries.", tag.RowsAffected())

	// B. The "Cleanup" Step:
	// If we successfully created merged rows, delete the old small ones.
	if tag.RowsAffected() > 0 {
		queryDelete := `
            DELETE FROM inverted_index 
            WHERE created_at::date = $1::date 
              AND segment_id NOT LIKE 'MERGED_%';
        `
		tagDel, err := tx.Exec(ctx, queryDelete, dateStr)
		if err != nil {
			return fmt.Errorf("failed to delete old segments: %w", err)
		}
		log.Printf("🗑️ Removed %d fragmented index entries.", tagDel.RowsAffected())
	}

	return tx.Commit(ctx)
}

// OptimizeIndex runs a VACUUM to physically reclaim disk space.
// Postgres leaves "dead tuples" (ghosts) after delete. This removes them.
func (a *Adapter) OptimizeIndex(ctx context.Context) error {
	// We cannot run VACUUM inside a transaction, so we use Exec directly.
	_, err := a.pool.Exec(ctx, "VACUUM ANALYZE inverted_index")
	return err
}
