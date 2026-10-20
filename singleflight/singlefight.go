package singleflight

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// ErrReentrant 表示同一个 goroutine 在 fn 执行期间，又对同一个 key 调用了 Do。
// 这种调用既不会执行 fn，又在等待自己这一层完成，永远无法解开，因此立即返回错误。
var ErrReentrant = errors.New("singleflight: re-entrant Do for the same key")

// 同一个 key 大量并发请求同时缓存 miss 时，只放行 1 个协程去回源查询数据库，剩下所有并发协程阻塞等待，复用这一次查询结果，避免大量请求同时打到底层 DB。
// call：代表一次【正在执行 / 已经执行完成】的请求
type call struct {
	wg     sync.WaitGroup // 等待组，让其他协程阻塞等待请求结束
	val    interface{}    // 请求成功的结果
	err    error          // 请求的错误信息
	leader uint64         // 正在执行 fn 的 goroutine ID，用于识别递归调用
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
		leader := c.leader
		g.mu.Unlock()
		// 同一个 goroutine 在 fn 里递归调用同一个 key：它既不会执行 fn，
		// 又在等待自己这一层完成，必须立刻失败而不是阻塞。
		if id := goroutineID(); id != 0 && id == leader {
			return nil, fmt.Errorf("%w: key %q", ErrReentrant, key)
		}
		return c.wait()
	}
	c := &call{leader: goroutineID()}
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

// goroutineID 解析 runtime.Stack 的首行（"goroutine 123 [running]:"）得到当前
// goroutine 的 ID。Go 没有公开的 goroutine-local storage 或 goroutine ID API，
// 这里只在 key 已经 in-flight 时调用，用于区分「同一 goroutine 的递归调用」和
// 「其他 goroutine 的正常等待」。解析失败返回 0，调用方退化为普通等待语义。
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) {
		return 0
	}
	var id uint64
	for _, b := range buf[len(prefix):n] {
		if b < '0' || b > '9' {
			break
		}
		id = id*10 + uint64(b-'0')
	}
	return id
}
