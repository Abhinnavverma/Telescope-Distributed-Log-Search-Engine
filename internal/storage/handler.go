package storage

import (
	"context"
	"log"

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
	// 1. Validate (Optional but good practice)
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

// ...existing code...
func (h *GRPCHandler) SearchLogs(ctx context.Context, req *proto.SearchLogRequest) (*proto.SearchLogResponse, error) {
	// 1. Validation
	if req.Query == "" {
		// Empty query? Return empty results (or error, your choice)
		return &proto.SearchLogResponse{Results: []*proto.LogEntry{}}, nil // Changed Entries -> Results
	}

	// 2. Call Business Logic (Read Path)
	// Pass StartTime and EndTime from request
	logs, err := h.svc.Search(ctx, req.Query, req.StartTime, req.EndTime)
	if err != nil {
		log.Printf("Error searching logs: %v", err)
		return nil, err
	}

	// 3. Convert Domain Models -> Proto Models
	// The Service returns internal structs; we need to map them back to Proto.
	protoLogs := make([]*proto.LogEntry, len(logs))
	for i, l := range logs {
		protoLogs[i] = &proto.LogEntry{
			Id:        l.Id,
			TraceId:   l.TraceId,
			Service:   l.Service,
			Level:     l.Level,
			Body:      l.Body,
			Timestamp: l.Timestamp,
		}
	}

	return &proto.SearchLogResponse{Results: protoLogs}, nil // Changed Entries -> Results
}
