package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/internal/storage"
	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/internal/storage/postgres"
	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	log.Println("🔭 Telescope Storage Node starting...")

	// 1. Configuration (Env Vars with Defaults)
	dbURL := getEnv("DB_URL", "postgres://user:password@localhost:5432/telescope")
	grpcPort := getEnv("GRPC_PORT", ":9000")

	// Kafka Config
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "telescope-logs")
	kafkaGroup := getEnv("KAFKA_GROUP_ID", "telescope-group-v1")

	// 2. Connect to Postgres
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()
	log.Println("✅ Connected to Postgres")

	// 3. Initialize Components (The Assembly Line)

	// A. Data Layer (Disk)
	repo := postgres.NewAdapter(pool)

	// B. Service Layer (Logic + RAM)
	// We pass the kafkaGroup so the service knows who to checkpoint as.
	svc := storage.NewStorageService(repo, kafkaGroup)

	bgWorkerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerCfg := storage.WorkerConfig{
		RetentionPeriod: 7 * 24 * time.Hour, // Keep 7 days
		JanitorInterval: 1 * time.Hour,      // Check every hour
		MergerInterval:  24 * time.Hour,     // Merge once a day
	}

	svc.StartWorkers(bgWorkerCtx, workerCfg)

	// C. Network Layer 1: gRPC (The "Read" Path & Test Path)
	// Note: We use storage.NewGRPCHandler, importing it from the internal package!
	handler := storage.NewGRPCHandler(svc)

	// D. Network Layer 2: Kafka (The "Write" Path)
	kafkaCfg := storage.ConsumerConfig{
		Brokers: kafkaBrokers,
		Topic:   kafkaTopic,
		GroupID: kafkaGroup,
	}
	consumer := storage.NewKafkaConsumer(kafkaCfg, svc)

	// 4. Start Background Processes

	// Start Kafka Consumer (Non-blocking here, runs in goroutine)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("❌ Kafka Consumer stopped: %v", err)
		}
	}()

	// 5. Setup gRPC Server
	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterLogServiceServer(grpcServer, handler)

	// 6. Graceful Shutdown
	// This ensures we don't lose data when we restart the server.
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Println("\n  Shutting down...")

		// A. Stop accepting new gRPC requests
		grpcServer.GracefulStop()

		// B. Stop pulling new Kafka logs
		consumer.Close()

		// C. Flush whatever is in RAM to Disk
		log.Println(" Flushing remaining logs to Disk...")
		svc.Flush()

		log.Println(" Goodnight.")
		os.Exit(0)
	}()

	log.Printf(" Storage Node listening on %s", grpcPort)
	log.Printf(" Ingesting from Kafka Topic: %s (Group: %s)", kafkaTopic, kafkaGroup)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Helper to read environment variables
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
