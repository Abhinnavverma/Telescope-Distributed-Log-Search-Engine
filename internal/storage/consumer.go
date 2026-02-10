package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
	"github.com/segmentio/kafka-go"
)

// KafkaConsumer manages the connection to the Kafka cluster
type KafkaConsumer struct {
	reader  *kafka.Reader
	service *StorageService
}

// Config holds the connection details (No more hardcoding!)
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// NewKafkaConsumer initializes the reader but does not start polling yet.
func NewKafkaConsumer(cfg ConsumerConfig, svc *StorageService) *KafkaConsumer {
	// We use the segmentio/kafka-go library which handles
	// rebalancing and connection pooling automatically.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		CommitInterval: 0, // We manage offsets manually via Sync
		StartOffset:    kafka.FirstOffset,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})

	return &KafkaConsumer{
		reader:  r,
		service: svc,
	}
}

// Start runs the consumer loop (Blocking Call)
func (k *KafkaConsumer) Start(ctx context.Context) error {
	log.Println("🔌 Consumer Loop Started. Waiting for logs...")
	var lastOffset int64
	var err error
	maxRetries := 10 // Try for 20 seconds (10 * 2s)

	for i := 0; i < maxRetries; i++ {
		// Try to get the checkpoint
		lastOffset, err = k.service.GetLastOffset(ctx, k.reader.Config().Topic, 0)

		if err == nil {
			break // Success! Exit loop.
		}

		// If failed, log and wait
		log.Printf("⚠️  DB not ready yet (Attempt %d/%d): %v", i+1, maxRetries, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second): // Wait 2s
		}
	}

	// If we still failed after 10 tries, then we crash.
	if err != nil {
		return fmt.Errorf("❌ CRITICAL: Could not connect to DB after %d retries: %w", maxRetries, err)
	}

	if lastOffset != -1 {
		log.Printf("⏮️  Rewinding Kafka to Offset %d (Trusting Postgres)", lastOffset+1)

		// We set the offset to last_offset + 1 (the NEXT message)
		// Note: SetOffset must be called before the first Read/Fetch
		err = k.reader.SetOffset(lastOffset + 1)
		if err != nil {
			return fmt.Errorf("failed to seek kafka: %v", err)
		}
	} else {
		log.Println("🆕 No checkpoint found. Starting from Kafka default (First/Last).")
	}

	for {
		// 1. Fetch Message (Blocks until data arrives or ctx is cancelled)
		m, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // Context cancelled, clean exit
			}
			log.Printf("❌ Error fetching message: %v", err)
			continue // Temporary network blip? Retry.
		}

		// 2. Deserialize (JSON -> Proto)
		// We assume the Gateway sends JSON. If it sends Protobuf binary, use proto.Unmarshal
		var entry proto.LogEntry
		if err := json.Unmarshal(m.Value, &entry); err != nil {
			log.Printf("⚠️ Failed to parse log (Offset %d): %v", m.Offset, err)
			continue // Skip bad data (poison pill)
		}

		// 3. Push to MemTable (WITH OFFSET)
		// We pass the Kafka Offset so the Service can checkpoint it.
		// If the Service returns an error (e.g. MemTable full), we might want to backoff.
		err = k.service.Push(ctx, []*proto.LogEntry{&entry}, m.Offset)
		if err != nil {
			log.Printf("⚠️ Service refused log: %v", err)
			// In a real system, we would pause/retry here to avoid dropping data.
		}
	}
}

// Close safely shuts down the connection
func (k *KafkaConsumer) Close() error {
	log.Println("🔌 Closing Kafka Consumer...")
	return k.reader.Close()
}
