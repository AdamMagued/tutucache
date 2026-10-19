package geecache

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// =============================================================================
// 按 Group 隔离的指标
//
// 每个 Group 持有独立的 Metrics 实例，避免多个命名空间互相污染。
// 写入与快照共用同一把锁，保证 Snapshot 是某一时刻的一致视图。
// =============================================================================

type Metrics struct {
	mu         sync.RWMutex
	hits       int64 // 命中次数
	misses     int64 // 未命中次数
	localLoads int64 // 从本地数据源加载次数
	peerLoads  int64 // 从远程节点加载次数
	evictions  int64 // 淘汰条目数
}

// MetricsSnapshot 是 Metrics 在某一时刻的值拷贝。
// 单独定义而不复用 Metrics，避免拷贝内含的 RWMutex（go vet copylocks）。
type MetricsSnapshot struct {
	Hits       int64
	Misses     int64
	LocalLoads int64
	PeerLoads  int64
	Evictions  int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

// =============================================================================
// 写入方法 — 在关键路径上由 Group / cache 调用
// =============================================================================

func (m *Metrics) RecordHit()       { m.add(&m.hits) }
func (m *Metrics) RecordMiss()      { m.add(&m.misses) }
func (m *Metrics) RecordLocalLoad() { m.add(&m.localLoads) }
func (m *Metrics) RecordPeerLoad()  { m.add(&m.peerLoads) }
func (m *Metrics) RecordEviction()  { m.add(&m.evictions) }

func (m *Metrics) add(p *int64) {
	m.mu.Lock()
	*p++
	m.mu.Unlock()
}

// =============================================================================
// 读取方法 — 供外部查询
// =============================================================================

// HitRate 返回进程生命周期内的命中率（lifetime ratio），不是近期窗口速率。
// /metrics 不再导出该值；需要近期趋势请在 Prometheus 侧对原始计数器求 rate()。
func (m *Metrics) HitRate() float64 {
	s := m.Snapshot() // hits/misses 必须来自同一份快照
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Snapshot 在锁内一次性拷贝全部字段，返回某一时刻的一致视图。
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return MetricsSnapshot{
		Hits:       m.hits,
		Misses:     m.misses,
		LocalLoads: m.localLoads,
		PeerLoads:  m.peerLoads,
		Evictions:  m.evictions,
	}
}

// =============================================================================
// HTTP 端点 — 注册到 HTTPPool 的路由中
// =============================================================================

type groupMetrics struct {
	name string
	snap MetricsSnapshot
}

// MetricsHandler 返回一个 http.Handler，按 group 标签暴露 Prometheus 风格指标。
func MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 只在全局锁内拷贝 Group 指针，格式化输出时不持有全局锁
		mu.RLock()
		gs := make([]*Group, 0, len(groups))
		for _, g := range groups {
			gs = append(gs, g)
		}
		mu.RUnlock()

		// map 遍历顺序随机，排序保证输出稳定
		sort.Slice(gs, func(i, j int) bool { return gs[i].name < gs[j].name })

		rows := make([]groupMetrics, 0, len(gs))
		for _, g := range gs {
			rows = append(rows, groupMetrics{name: g.name, snap: g.metrics.Snapshot()})
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		writeCounter(w, "geecache_hits_total", "Total number of cache hits", rows, func(s MetricsSnapshot) int64 { return s.Hits })
		writeCounter(w, "geecache_misses_total", "Total number of cache misses", rows, func(s MetricsSnapshot) int64 { return s.Misses })
		writeCounter(w, "geecache_local_loads_total", "Total number of local loads", rows, func(s MetricsSnapshot) int64 { return s.LocalLoads })
		writeCounter(w, "geecache_peer_loads_total", "Total number of peer loads", rows, func(s MetricsSnapshot) int64 { return s.PeerLoads })
		writeCounter(w, "geecache_evictions_total", "Total number of evictions", rows, func(s MetricsSnapshot) int64 { return s.Evictions })
	})
}

// writeCounter 输出一个指标族：HELP/TYPE 各一次，然后每个 group 一条序列。
func writeCounter(w http.ResponseWriter, name, help string, rows []groupMetrics, value func(MetricsSnapshot) int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	for _, r := range rows {
		fmt.Fprintf(w, "%s{group=%q} %d\n", name, r.name, value(r.snap))
	}
}

//cc
