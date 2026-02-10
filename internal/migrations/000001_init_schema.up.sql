CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. LOGS TABLE
-- ID is ULID (Time-sortable, Unique). 
-- TraceID is UUID (Random, for correlation).
CREATE TABLE IF NOT EXISTS logs (
    id          TEXT PRIMARY KEY,  -- ULID. Sole PK.
    trace_id    TEXT NOT NULL,     -- UUID. Just a column.
    service     TEXT,
    level       TEXT,
    body        TEXT,
    created_at  TIMESTAMPTZ NOT NULL
);

-- Indexes for fast filtering
CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);
CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id);
CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service);


-- 2. INVERSE INDEX TABLE (Segment-Based / LSM Style)
-- Matches your "Segment" design. 
-- A Keyword can appear in multiple Segments.
CREATE TABLE IF NOT EXISTS inverted_index (
    segment_id  TEXT NOT NULL,    -- The "Batch ID" (ULID of the flush)
    keyword     TEXT NOT NULL,    -- "Word" in your diagram
    log_ids     TEXT[] NOT NULL,  -- The Array of IDs
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    
    -- Composite PK: Uniquely identifies a "Word Block" inside a "Segment"
    PRIMARY KEY (segment_id, keyword)
);

-- Index on keyword allows finding ALL segments containing "error" quickly
CREATE INDEX IF NOT EXISTS idx_inverted_keyword ON inverted_index(keyword);


-- 3. KAFKA CHECKPOINT TABLE
-- Matches your 3-part Primary Key for partition awareness
CREATE TABLE IF NOT EXISTS kafka_checkpoints (
    reader_group    TEXT NOT NULL,  -- "Reader"
    topic           TEXT NOT NULL,  -- "ReadingTopic"
    partition_id    INT NOT NULL,   -- "Partition_id"
    last_offset     BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (reader_group, topic, partition_id)
);