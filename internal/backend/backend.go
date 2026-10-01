package backend

import (
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type Backend struct{ 
	URL *url.URL 
	Healthy bool
	ActiveRequests atomic.Int64
	Stats BackendStats
}


type BackendStats struct {
	Request uint64
	Errors uint64
	TotalLatency time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
}
