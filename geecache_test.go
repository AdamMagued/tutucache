package geecache

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get("key"); !reflect.DeepEqual(v, expect) {
		t.Errorf("callback failed")
	}

}

func TestMetrics(t *testing.T) {
	// 创建很小的缓存（10 字节），确保写入几个 key 后就触发淘汰
	g := NewGroup("m", 10, GetterFunc(
		func(key string) ([]byte, error) {
			return []byte("hello-" + key), nil // 每个 value 约 7-8 字节
		},
	))

	// 第一次 Get("a"): miss → local load → 缓存 a（key 1B + value 7B = 8B）
	g.Get("a")
	// 第二次 Get("a"): hit
	g.Get("a")
	// Get("b"): miss → local load → 写入 b（总 16B > 10B → 淘汰 a）
	g.Get("b")
	// Get("c"): miss → local load → 写入 c（再淘汰一个）
	g.Get("c")

	s := g.Metrics()
	if s.Hits != 1 {
		t.Fatalf("hits=%d, want 1", s.Hits)
	}
	if s.Misses != 3 {
		t.Fatalf("misses=%d, want 3", s.Misses)
	}
	if s.LocalLoads != 3 {
		t.Fatalf("local loads=%d, want 3", s.LocalLoads)
	}
	if s.Evictions != 2 {
		t.Fatalf("evictions=%d, want 2", s.Evictions)
	}
	rate := g.metrics.HitRate()
	if rate != 0.25 {
		t.Fatalf("lifetime hit rate=%.4f, want 0.25", rate)
	}
	t.Logf("hits=%d misses=%d loads=%d evictions=%d hit_rate=%.2f",
		s.Hits, s.Misses, s.LocalLoads, s.Evictions, rate)
}

func TestMetricsPerGroup(t *testing.T) {
	getter := GetterFunc(func(key string) ([]byte, error) {
		return []byte("v-" + key), nil
	})
	a := NewGroup("metrics-group-a", 1<<20, getter)
	b := NewGroup("metrics-group-b", 1<<20, getter)

	a.Get("k") // miss + local load
	a.Get("k") // hit

	as, bs := a.Metrics(), b.Metrics()
	if as.Hits != 1 || as.Misses != 1 || as.LocalLoads != 1 {
		t.Fatalf("group a metrics changed: %+v", as)
	}
	if bs != (MetricsSnapshot{}) {
		t.Fatalf("group b must stay isolated, got %+v", bs)
	}
	if a.metrics == b.metrics {
		t.Fatal("groups must not share a Metrics instance")
	}
}

func TestMetricsHandlerPerGroupLabels(t *testing.T) {
	getter := GetterFunc(func(key string) ([]byte, error) {
		return []byte("v-" + key), nil
	})
	a := NewGroup("metrics-handler-a", 1<<20, getter)
	b := NewGroup("metrics-handler-b", 1<<20, getter)

	a.Get("x") // miss
	a.Get("x") // hit
	b.Get("y") // miss

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("Content-Type=%q, want text/plain", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`geecache_hits_total{group="metrics-handler-a"} 1`,
		`geecache_misses_total{group="metrics-handler-a"} 1`,
		`geecache_local_loads_total{group="metrics-handler-a"} 1`,
		`geecache_hits_total{group="metrics-handler-b"} 0`,
		`geecache_misses_total{group="metrics-handler-b"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}

	if strings.Contains(body, "geecache_hit_rate") {
		t.Fatal("lifetime hit-rate gauge must not be exported")
	}
	if n := strings.Count(body, "# TYPE geecache_hits_total counter"); n != 1 {
		t.Fatalf("HELP/TYPE for hits emitted %d times, want 1", n)
	}
}

func TestMetricsConcurrentRecording(t *testing.T) {
	const (
		workers   = 8
		perWorker = 1000
	)
	m := NewMetrics()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				m.RecordHit()
				m.RecordMiss()
				_ = m.Snapshot()
			}
		}()
	}
	wg.Wait()

	s := m.Snapshot()
	if s.Hits != workers*perWorker || s.Misses != workers*perWorker {
		t.Fatalf("snapshot=%+v, want hits=misses=%d", s, workers*perWorker)
	}
}

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup("scores", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		} // load from callback function
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		} // cache hit
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}
