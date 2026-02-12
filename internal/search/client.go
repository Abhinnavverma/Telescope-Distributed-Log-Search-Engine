package search_api

import (
	"context"
	"fmt"
	"time"

	"github.com/Abhinnavverma/Telescope-Distributed-Log-Search-Engine/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// StorageClient defines how we talk to a storage node
type StorageClient interface {
	Search(ctx context.Context, query string, start, end int64, limit int64, service string, level string) ([]*proto.LogEntry, error)
	Close() error
}

// grpcClient is the concrete implementation
type grpcClient struct {
	conn   *grpc.ClientConn
	client proto.LogServiceClient
	addr   string
}

func NewGRPCClient(addr string) (StorageClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to storage node %s: %w", addr, err)
	}

	return &grpcClient{
		conn:   conn,
		client: proto.NewLogServiceClient(conn),
		addr:   addr,
	}, nil
}

// CHANGED: Implementation of new signature
func (g *grpcClient) Search(ctx context.Context, query string, start, end int64, limit int64, service string, level string) ([]*proto.LogEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	fmt.Printf("OUTGOING gRPC: Query='%s' Service='%s' Limit=%d\n", query, service, limit)
	req := &proto.SearchLogRequest{
		Query:     query,
		StartTime: start,
		EndTime:   end,
		Limit:     int32(limit),
		Service:   service,
		Level:     level,
	}

	resp, err := g.client.SearchLogs(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("node %s failed: %w", g.addr, err)
	}
	return resp.Results, nil
}

func (g *grpcClient) Close() error {
	return g.conn.Close()
}
