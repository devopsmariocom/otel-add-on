package metric

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"

	ty "github.com/kedify/otel-add-on/types"
)

func countSeries(store ty.MemStore, name string) int {
	series, found := store.GetStore().Load(name)
	if !found {
		return 0
	}
	n := 0
	series.Range(func(ty.LabelsHash, *ty.MetricData) bool {
		n++
		return true
	})
	return n
}

func backdate(store ty.MemStore, name string, by time.Duration) {
	series, found := store.GetStore().Load(name)
	if !found {
		return
	}
	series.Range(func(_ ty.LabelsHash, md *ty.MetricData) bool {
		md.LastWrite = md.LastWrite.Add(-by)
		return true
	})
}

func TestCollectStaleRemovesIdleSeriesAndEmptyMetrics(t *testing.T) {
	ms := newMetricStore(120, false, false)
	setupMetrics(ms, "m1", 1, map[string]any{"pod": "a"}, 1.)
	setupMetrics(ms, "m1", 1, map[string]any{"pod": "b"}, 1.)
	setupMetrics(ms, "m2", 1, map[string]any{"pod": "c"}, 1.)

	if removed := ms.CollectStale(time.Now(), 10*time.Minute); removed != 0 {
		t.Fatalf("fresh series must survive, removed %d", removed)
	}

	backdate(ms, "m2", 20*time.Minute)
	if removed := ms.CollectStale(time.Now(), 10*time.Minute); removed != 1 {
		t.Fatalf("expected the idle m2 series to be removed, removed %d", removed)
	}
	if _, found := ms.GetStore().Load("m2"); found {
		t.Error("metric name with no series left should be dropped")
	}
	if n := countSeries(ms, "m1"); n != 2 {
		t.Errorf("m1 series must be untouched, got %d", n)
	}
}

func TestCollectStaleKeepsSeriesThatKeepReceivingWrites(t *testing.T) {
	ms := newMetricStore(120, false, false)
	labels := map[string]any{"pod": "a"}
	setupMetrics(ms, "m1", 1, labels, 1.)
	backdate(ms, "m1", 20*time.Minute)
	setupMetrics(ms, "m1", 1, labels, 2.)

	if removed := ms.CollectStale(time.Now(), 10*time.Minute); removed != 0 {
		t.Errorf("a rewritten series is not idle, removed %d", removed)
	}
}

func TestCollectStaleDisabledWhenZero(t *testing.T) {
	ms := newMetricStore(120, false, false)
	setupMetrics(ms, "m1", 1, map[string]any{"pod": "a"}, 1.)
	backdate(ms, "m1", 24*time.Hour)
	if removed := ms.CollectStale(time.Now(), 0); removed != 0 {
		t.Errorf("zero must disable cleanup, removed %d", removed)
	}
}

func TestGetDropsStaleSeriesOnVectorPath(t *testing.T) {
	ms := newMetricStore(5, false, false)
	now := time.Now().Unix()
	setupMetricsWithNow(ms, now-1000, "m1", 1, map[string]any{"a": "1", "pod": "p1"}, 1.)
	setupMetricsWithNow(ms, now-1000, "m1", 1, map[string]any{"a": "1", "pod": "p2"}, 1.)
	setupMetricsWithNow(ms, now, "m1", 1, map[string]any{"a": "1", "pod": "p3"}, 7.)

	val, found, err := ms.Get("m1", map[string]any{"a": "1"}, ty.OpLastOne, ty.VecSum)
	assertMetricFound(t, val, found, err, 7.)
	if n := countSeries(ms, "m1"); n != 1 {
		t.Errorf("stale series must be evicted by the read that finds them, %d left", n)
	}
}

func TestInternalMetricsCollectStale(t *testing.T) {
	m := &InternalMetrics{}
	m.Init()
	reg := prometheus.NewRegistry()
	reg.MustRegister(m.metrics.KedaOtelScalerValue, m.metrics.KedaOtelScalerValueClamped)
	gauges := func() int {
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, mf := range mfs {
			n += len(mf.GetMetric())
		}
		return n
	}

	m.SetMetricValue("n", "l", "last_one", "sum", 1)
	m.SetMetricValueClamped("n", "l", "last_one", "sum", "so", "ns", 1)
	if got := gauges(); got != 2 {
		t.Fatalf("expected 2 gauges, got %d", got)
	}
	if removed := m.CollectStale(time.Now(), 10*time.Minute); removed != 0 || gauges() != 2 {
		t.Fatalf("fresh gauges must survive, removed %d", removed)
	}
	if removed := m.CollectStale(time.Now().Add(time.Hour), 10*time.Minute); removed != 2 || gauges() != 0 {
		t.Fatalf("idle gauges must be removed, removed %d, left %d", removed, gauges())
	}
}

func TestRunGarbageCollector(t *testing.T) {
	ms := newMetricStore(120, false, false)
	m := &InternalMetrics{}
	m.Init()
	setupMetrics(ms, "m1", 1, map[string]any{"pod": "a"}, 1.)
	backdate(ms, "m1", time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunGarbageCollector(ctx, ms, m, 50*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for countSeries(ms, "m1") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("GC did not remove the idle series")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not stop on context cancellation")
	}
}

func TestRunGarbageCollectorDisabled(t *testing.T) {
	done := make(chan struct{})
	go func() {
		RunGarbageCollector(context.Background(), newMetricStore(120, false, false), &InternalMetrics{}, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("disabled GC must return immediately")
	}
}

func TestLazyStoreConcurrentAccess(t *testing.T) {
	ms := newMetricStore(120, true, true)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				ms.Put(ty.NewMetricEntry{
					Name:             ty.MetricName("m" + string(rune('a'+i%5))),
					MeasurementTime:  pcommon.Timestamp(time.Now().Unix()),
					MeasurementValue: float64(i),
					Labels:           map[string]any{"pod": string(rune('a' + i%7))},
				})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_, _, _ = ms.Get(ty.MetricName("m"+string(rune('a'+i%5))), map[string]any{}, ty.OpLastOne, ty.VecSum)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				ms.CollectStale(time.Now(), time.Hour)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				ms.IsSubscribed(true, "ma", ty.OpLastOne)
			}
		}()
	}
	wg.Wait()
}
