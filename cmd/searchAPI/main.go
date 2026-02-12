package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	search_api "github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/internal/search"
)

func main() {
	// 1. Configuration
	addr := getEnv("HTTP_PORT", ":8080")
	nodeList := getEnv("STORAGE_NODES", "localhost:9000") // Comma separated list

	// 2. Infrastructure (Clients)
	var clients []search_api.StorageClient
	nodes := strings.Split(nodeList, ",")

	for _, nodeAddr := range nodes {
		log.Printf("🔌 Connecting to Storage Node: %s", nodeAddr)
		client, err := search_api.NewGRPCClient(nodeAddr)
		if err != nil {
			log.Fatalf("Failed to create client for %s: %v", nodeAddr, err)
		}
		defer client.Close()
		clients = append(clients, client)
	}

	// 3. Service Layer (Business Logic)
	svc := search_api.NewSearchService(clients)

	// 4. Transport Layer (HTTP Handler)
	handler := search_api.NewSearchHandler(svc)

	// 5. Server
	http.Handle("/search", handler)

	log.Printf("🚀 SearchAPI listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf(" Server crashed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
