package metric

import (
	"context"
	"time"

	"github.com/kedify/otel-add-on/types"
)

// RunGarbageCollector periodically drops series and exported gauges idle for longer than staleAfter; zero disables it.
func RunGarbageCollector(ctx context.Context, store types.MemStore, metrics *InternalMetrics, staleAfter time.Duration) {
	if staleAfter <= 0 {
		return
	}
	ticker := time.NewTicker(min(time.Minute, staleAfter))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			metrics.CollectStale(now, staleAfter)
			store.CollectStale(now, staleAfter)
		}
	}
}
