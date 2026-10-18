package singleflight

import (
	"fmt"
	"sync"
)

// 同一个 key 大量并发请求同时缓存 miss 时，只放行 1 个协程去回源查询数据库，剩下所有并发协程阻塞等待，复用这一次查询结果，避免大量请求同时打到底层 DB。
// call：代表一次【正在执行 / 已经执行完成】的请求
type call struct {
	wg  sync.WaitGroup // 等待组，让其他协程阻塞等待请求结束
	val interface{}    // 请求成功的结果
	err error          // 请求的错误信息
}

// Group：singleflight管理器，维护所有key对应的请求
type Group struct {
	mu sync.Mutex       // 保护map m并发安全
	m  map[string]*call // key -> 对应的请求实例call
}

// 构造函数，初始化内部map
func NewGroup() *Group {
	return &Group{
		m: make(map[string]*call),
	}
}

// 针对相同key， 无论Do被调用多少次， 函数fn都只会被调用一次
// 等fn调用结束了， 返回返回值或者err
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	// 兜底懒加载（防止忘记调用NewGroup）
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 这个key已经有任务正在执行
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		return c.wait()
	}
	c := &call{}
	//只有第一个 goroutine 去启动任务，给 wg+1；
	// 其他 goroutine 全部阻塞等待这个任务完成。
	// 任务 Done 之后，所有等待 goroutine 被唤醒，拿到同一份返回值。wg 计数器记录的是【正在执行的任务数】，不是等待协程数量。
	c.wg.Add(1) //请求前加锁
	g.m[key] = c
	g.mu.Unlock()

	g.doCall(c, key, fn)
	return c.val, c.err
}

// doCall 执行 fn，并保证无论正常返回还是 panic，都会删除 key 并唤醒所有等待者。
func (g *Group) doCall(c *call, key string, fn func() (interface{}, error)) {
	defer func() {
		// recover 必须直接在 defer 中调用才能捕获 panic。
		r := recover()
		if r != nil {
			// 给等待者一个明确的错误，避免它们拿到 (nil, nil) 误以为成功。
			c.err = fmt.Errorf("singleflight: fn panicked: %v", r)
		}

		// 先删除 key 再 Done：新的 Do(key) 会创建新的 call，
		// 不会拿到这个已经结束（或已 panic）的 call。
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()

		c.wg.Done() // 唤醒所有阻塞在 c.wait() 的调用方

		// 清理完成后再把 panic 抛给执行 fn 的调用方。
		if r != nil {
			panic(r)
		}
	}()

	c.val, c.err = fn()
}

func (c *call) wait() (interface{}, error) {
	c.wg.Wait()
	return c.val, c.err
}
