package search_api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type SearchHandler struct {
	service *SearchService
}

func NewSearchHandler(svc *SearchService) *SearchHandler {
	return &SearchHandler{service: svc}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// 1. Mandatory Query
	query := q.Get("q")

	serviceFilter := q.Get("service")
	fmt.Printf("🔍 [1] HANDLER: Received service='%s'\n", serviceFilter)
	levelFilter := q.Get("level")

	// 2. Parse Optional Params (With Defaults)

	// Default: Last 1 hour
	end := time.Now().UnixNano()
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	limit := 100

	if t := q.Get("start"); t != "" {
		if v, err := strconv.ParseInt(t, 10, 64); err == nil {
			start = v
		}
	}
	if t := q.Get("end"); t != "" {
		if v, err := strconv.ParseInt(t, 10, 64); err == nil {
			end = v
		}
	}
	if l := q.Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	startTime := time.Now()

	// 3. Call Service
	logs, err := h.service.Search(r.Context(), query, start, end, limit, serviceFilter, levelFilter)
	if err != nil {
		http.Error(w, "Search failed", http.StatusInternalServerError)
		return
	}

	// 4. Respond
	response := map[string]interface{}{
		"data":      logs,
		"count":     len(logs),
		"limit":     limit,
		"time_took": time.Since(startTime).String(),
	}

	w.Header().Set("Content-Type", "application/json")
	// Allow CORS for frontend dev
	w.Header().Set("Access-Control-Allow-Origin", "*") //

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}
