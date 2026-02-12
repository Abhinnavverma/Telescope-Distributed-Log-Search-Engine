package storage

import (
	"context"
	"log"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
)

// GRPCHandler acts as the "Controller" in MVC terms.
// It receives the raw gRPC request, unpacks it, and calls the Service.
type GRPCHandler struct {
	proto.UnimplementedLogServiceServer
	svc *StorageService // Dependency Injection
}

// NewGRPCHandler creates a handler with the injected service.
func NewGRPCHandler(svc *StorageService) *GRPCHandler {
	return &GRPCHandler{svc: svc}
}

// PushLogs implementation
func (h *GRPCHandler) PushLogs(ctx context.Context, req *proto.PushLogRequest) (*proto.PushLogResponse, error) {
	if len(req.Entries) == 0 {
		return &proto.PushLogResponse{Success: false}, nil
	}

	// 2. Call Business Logic
	err := h.svc.Push(ctx, req.Entries, -1)
	if err != nil {
		log.Printf("Error in PushLogs: %v", err)
		return &proto.PushLogResponse{Success: false}, err
	}

	// 3. Return Response
	return &proto.PushLogResponse{Success: true}, nil
}

// SearchLogs implementation
func (h *GRPCHandler) SearchLogs(ctx context.Context, req *proto.SearchLogRequest) (*proto.SearchLogResponse, error) {

	start := req.StartTime
	end := req.EndTime
	limit := int(req.Limit)

	// Default End: Now
	if end == 0 {
		end = time.Now().UnixNano()
	}
	// Default Start: 1 hour ago
	if start == 0 {
		start = time.Now().Add(-1 * time.Hour).UnixNano()
	}
	// Default Limit: 100
	if limit <= 0 {
		limit = 100
	}
	// Safety Cap
	if limit > 1000 {
		limit = 1000
	}

	// 2. Call Business Logic with sanitized params
	logs, err := h.svc.Search(ctx, req.Query, start, end, limit, req.Service, req.Level)
	if err != nil {
		log.Printf("Error searching logs: %v", err)
		return nil, err
	}

	// 3. Map to Response
	return &proto.SearchLogResponse{Results: logs}, nil
}
