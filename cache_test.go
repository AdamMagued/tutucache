package geecache

import (
	"testing"
	"time"
)

// cleanupChannels 返回 cache 当前清理循环的 stop/done 通道，供测试断言。
func cleanupChannels(c *cache) (stop, done chan struct{}) {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	return c.cleanupStop, c.cleanupDone
}

func TestStartCleanupIsIdempotent(t *testing.T) {
	c := &cache{}
	c.StartCleanup(time.Hour)

	stop, done := cleanupChannels(c)
	if stop == nil || done == nil {
		t.Fatal("cleanup loop did not start")
	}

	for i := 0; i < 100; i++ {
		c.StartCleanup(time.Hour)
	}

	stop2, done2 := cleanupChannels(c)
	if stop2 != stop || done2 != done {
		t.Fatal("repeated StartCleanup replaced the running cleanup loop")
	}

	c.StopCleanup()
	select {
	case <-done:
	default:
		t.Fatal("cleanup loop still running after StopCleanup")
	}
}

func TestStartCleanupIgnoresNonPositiveInterval(t *testing.T) {
	c := &cache{}
	c.StartCleanup(0)
	c.StartCleanup(-time.Second)

	if stop, _ := cleanupChannels(c); stop != nil {
		t.Fatal("cleanup loop started for a non-positive interval")
	}
}

func TestStopCleanupIsIdempotent(t *testing.T) {
	// 从未启动过清理循环时调用也必须是安全的。
	c := &cache{}
	c.StopCleanup()
	c.StopCleanup()

	if stop, _ := cleanupChannels(c); stop != nil {
		t.Fatal("StopCleanup started a cleanup loop")
	}

	// StopCleanup 之后不允许重新启动。
	c.StartCleanup(time.Hour)
	if stop, _ := cleanupChannels(c); stop != nil {
		t.Fatal("StartCleanup restarted a loop after StopCleanup")
	}

	// 已启动的循环可以重复 Stop，且第一次 Stop 返回时 goroutine 已退出。
	c2 := &cache{}
	c2.StartCleanup(time.Hour)
	_, done := cleanupChannels(c2)
	c2.StopCleanup()
	c2.StopCleanup()
	select {
	case <-done:
	default:
		t.Fatal("cleanup loop still running after StopCleanup")
	}
}

func TestCleanupLoopRemovesExpiredEntries(t *testing.T) {
	c := &cache{}
	c.add("k", ByteView{b: []byte("v")}, 10*time.Millisecond)
	c.StartCleanup(5 * time.Millisecond)
	defer c.StopCleanup()

	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := 0
		if c.lru != nil {
			n = c.lru.Len()
		}
		c.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired entry was not removed, lru len=%d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGroupCloseStopsCleanup(t *testing.T) {
	g := NewGroup("cleanup-group-close", 1<<10, GetterFunc(func(k string) ([]byte, error) {
		return []byte(k), nil
	}))

	g.SetTTL(time.Hour, time.Hour)
	stop, done := cleanupChannels(&g.mainCache)
	if stop == nil || done == nil {
		t.Fatal("SetTTL did not start the cleanup loop")
	}

	// 重复 SetTTL 只更新 TTL，不会额外启动清理循环。
	for i := 0; i < 50; i++ {
		g.SetTTL(time.Hour, time.Hour)
	}
	stop2, done2 := cleanupChannels(&g.mainCache)
	if stop2 != stop || done2 != done {
		t.Fatal("repeated SetTTL started additional cleanup loops")
	}

	g.Close()
	g.Close() // 幂等

	select {
	case <-done:
	default:
		t.Fatal("Group.Close did not stop the cleanup loop")
	}

	// Close 只停止后台清理，不影响正常读取。
	if _, err := g.Get("a"); err != nil {
		t.Fatalf("Get after Close failed: %v", err)
	}
}
