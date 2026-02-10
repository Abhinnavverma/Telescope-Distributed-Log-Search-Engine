package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
	"github.com/segmentio/kafka-go"
)

// Config
const (
	Topic     = "telescope-logs"
	TotalLogs = 100000
	BatchSize = 5000
)

func main() {
	brokerAddr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokerAddr, ",")

	log.Printf("🔌 Connecting to Kafka at: %s", brokers[0])

	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    Topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	log.Println("🚀 Starting Log Generator (Turbo Mode)...")
	start := time.Now()

	// The "Box" to hold our messages
	var batch []kafka.Message

	for i := 0; i < TotalLogs; i++ {
		entry := generateLog(i)
		payload, _ := json.Marshal(entry)

		// 1. Pack the log into the box (Don't send yet!)
		batch = append(batch, kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: payload,
		})

		// 2. Is the box full? SHIP IT! 📦
		if len(batch) >= BatchSize {
			err := w.WriteMessages(context.Background(), batch...)
			if err != nil {
				log.Printf("❌ Failed to write batch: %v", err)
			}

			// Empty the box for the next round
			batch = nil
			fmt.Printf("Sent %d logs...\n", i+1)
		}
	}

	// 3. Flush whatever is left in the box
	if len(batch) > 0 {
		w.WriteMessages(context.Background(), batch...)
	}

	duration := time.Since(start)
	log.Printf("✅ DONE! Sent %d logs in %v", TotalLogs, duration)
	log.Printf("Rate: %.2f logs/sec", float64(TotalLogs)/duration.Seconds())
}

// --- Helpers (Same as before) ---
var services = []string{"payment-service", "auth-service", "frontend", "notification-worker"}
var levels = []string{"INFO", "WARN", "ERROR", "DEBUG"}

func generateLog(id int) *proto.LogEntry {
	return &proto.LogEntry{
		TraceId:   fmt.Sprintf("trace-%d", rand.Intn(100000)),
		Service:   services[rand.Intn(len(services))],
		Level:     levels[rand.Intn(len(levels))],
		Body:      fmt.Sprintf("User %d performed action %d", rand.Intn(1000), id),
		Timestamp: time.Now().UnixNano(),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
