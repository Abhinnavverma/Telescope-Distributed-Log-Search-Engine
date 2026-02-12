package storage

import (
	"context"
	"log"
	"time"
)

type WorkerConfig struct {
	RetentionPeriod time.Duration // e.g., 7 Days
	JanitorInterval time.Duration // e.g., Every 1 Hour
	MergerInterval  time.Duration // e.g., Every 6 Hours
}

// StartWorkers is the "Main Menu" for background tasks
func (s *StorageService) StartWorkers(ctx context.Context, cfg WorkerConfig) {
	go s.runJanitor(ctx, cfg)
	go s.runMerger(ctx, cfg)
}

// 1. The Janitor (Deletes old data)
func (s *StorageService) runJanitor(ctx context.Context, cfg WorkerConfig) {
	ticker := time.NewTicker(cfg.JanitorInterval)
	defer ticker.Stop()

	log.Println(" Janitor Worker Started.")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Calculate Cutoff (Now - 7 Days)
			cutoff := time.Now().Add(-cfg.RetentionPeriod)

			count, err := s.repo.DeleteOldLogs(ctx, cutoff)
			if err != nil {
				log.Printf(" Janitor Failed: %v", err)
			} else if count > 0 {
				log.Printf(" Janitor deleted %d expired logs.", count)
			}
		}
	}
}

// 2. The Merger (Compacts the Index)
func (s *StorageService) runMerger(ctx context.Context, cfg WorkerConfig) {
	ticker := time.NewTicker(cfg.MergerInterval)
	defer ticker.Stop()

	log.Println("🏗️ Merger Worker Started.")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// We always compact "Yesterday" (Cold Data).
			// Compacting "Today" is risky because it's still being written to.
			yesterday := time.Now().Add(-24 * time.Hour)

			// A. Compact the Segments
			if err := s.repo.CompactSegments(ctx, yesterday); err != nil {
				log.Printf(" Merger Failed: %v", err)
				continue
			}

			// B. Run Vacuum (Reclaim Disk Space)
			if err := s.repo.OptimizeIndex(ctx); err != nil {
				log.Printf(" Vacuum Failed: %v", err)
			}
		}
	}
}
