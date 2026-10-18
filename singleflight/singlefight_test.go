package singleflight

import (
	"sync/atomic"
	"testing"
	"time"
)

// recoverPanic 执行 f 并返回 panic 的值；没有 panic 时返回 nil。
func recoverPanic(f func()) (r interface{}) {
	defer func() { r = recover() }()
	f()
	return
}

// 回归测试 #2：fn panic 后，该 key 不能永久卡死。
func TestDoPanicDoesNotWedgeKey(t *testing.T) {
	g := NewGroup()

	r := recoverPanic(func() {
		_, _ = g.Do("k", func() (interface{}, error) {
			panic("boom")
		})
	})
	if r != "boom" {
		t.Fatalf("panic value = %v, want boom (panic should propagate to caller)", r)
	}

	// 同一个 key 必须可以再次使用，且不会被旧的 call 卡住。
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, err := g.Do("k", func() (interface{}, error) { return "ok", nil })
		if err != nil || v != "ok" {
			t.Errorf("Do after panic = (%v, %v), want (ok, nil)", v, err)
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Do(k) is permanently blocked after a panic in fn")
	}
}

// fn panic 时，已经阻塞的等待者必须被唤醒并拿到错误。
func TestDoPanicUnblocksWaiters(t *testing.T) {
	g := NewGroup()
	inFn := make(chan struct{})
	release := make(chan struct{})

	// winner 进入 fn 后阻塞，随后 panic。
	go func() {
		defer func() { _ = recover() }()
		_, _ = g.Do("k", func() (interface{}, error) {
			close(inFn)
			<-release
			panic("boom")
		})
	}()
	<-inFn

	// 取出正在执行的 call，模拟一个并发 Do 调用方阻塞在 c.wait() 上。
	g.mu.Lock()
	c := g.m["k"]
	g.mu.Unlock()
	if c == nil {
		t.Fatal("call entry missing while fn is running")
	}

	waiterErr := make(chan error, 1)
	go func() {
		_, err := c.wait()
		waiterErr <- err
	}()

	close(release)

	select {
	case err := <-waiterErr:
		if err == nil {
			t.Fatal("waiter got nil error, want a panic error")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter blocked forever after fn panicked")
	}
}

// 正常路径不能被改坏：并发同 key 仍然只执行一次，并共享结果。
// 测试目的：保证修改singleflight代码后，**基础功能不被破坏**
// 核心特性：多个goroutine并发请求同一个key，业务函数fn只会执行1次；其余请求等待并复用winner的结果
func TestDoDeduplicatesAndSharesResult(t *testing.T) {
	g := NewGroup()
	var calls int32                   // 原子计数器，统计【真正执行的业务fn总次数】，多协程并发安全
	inFn := make(chan struct{})       // 同步栅栏：winner协程进入业务fn内部时close，通知主测试协程
	release := make(chan struct{})    // 放行开关：winner会卡在 <-release，直到主测试close这个通道才继续往下跑
	winnerDone := make(chan struct{}) // 信号通道：winner的g.Do整段逻辑全部执行完毕后close，通知主测试

	// 启动winner协程：抢到key执行权的那个协程
	go func() {
		defer close(winnerDone) // 无论正常/异常，winner整个Do调用结束就关闭winnerDone，通知主线
		_, _ = g.Do("k", func() (interface{}, error) {
			atomic.AddInt32(&calls, 1) // 业务fn执行，计数器+1
			close(inFn)                // ✅通知主线程：我已经进入业务fn里面了！
			<-release                  // ==========关键点==========
			// winner阻塞在这里，暂停执行！用来制造场景：
			// winner正在跑fn，此时其他8个并发请求进来
			return "v", nil // 等到release被close之后，才会执行return
		})
	}()

	// 主线程阻塞在这里，直到winner执行 close(inFn)
	// 保证：下面8个协程启动的时候，winner已经进入业务fn，key已经存入singleflight的map
	<-inFn

	const n = 8
	results := make(chan interface{}, n) // 带缓冲通道，保存8个等待协程拿到的返回值；缓冲=8，写的时候不会阻塞
	errs := make(chan error, n)          // 带缓冲通道，保存8个等待协程拿到的error

	// 启动8个并发等待协程，全部请求同一个key "k"
	for i := 0; i < n; i++ {
		go func() {
			// singleflight逻辑：map里已经存在"k"的call，不会执行这里面的fn！直接等待winner结果
			v, err := g.Do("k", func() (interface{}, error) {
				atomic.AddInt32(&calls, 1)
				return "other", nil // 这个return "other" 理论上永远不会被执行！
			})
			results <- v // 拿到结果写入通道
			errs <- err
		}()
	}

	// 短暂休眠，给8个协程时间：让它们跑完Do前置逻辑，找到map中已存在的call，进入wait阻塞
	// ⚠️只在单元测试用！业务代码禁止用sleep做并发同步！
	time.Sleep(20 * time.Millisecond)

	close(release) // 放行！解除winner在 <-release 的阻塞，winner继续执行return "v"

	// 循环读取8个等待协程返回的结果，做断言校验
	for i := 0; i < n; i++ {
		v := <-results
		if v != "v" {
			t.Errorf("waiter result = %v, want v", v)
		}
		err := <-errs
		if err != nil {
			t.Errorf("waiter error = %v, want nil", err)
		}
	}

	<-winnerDone // 等待winner协程的g.Do完整跑完，全部清理完毕
	got := atomic.LoadInt32(&calls)
	if got != 1 { // 校验：业务fn总共只执行1次（只有winner里面的fn运行）
		t.Fatalf("fn ran %d times, want 1", got)
	}
}
