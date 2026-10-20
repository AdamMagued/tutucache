package geecache

import (
	"fmt"
	pb "geecache/geecachepb"
	"geecache/singleflight"
	"log"
	"sync"
	"time"
)

/*
                            是
接收 key --> 检查是否被缓存 -----> 返回缓存值 ⑴
                |  否                         是
                |-----> 是否应当从远程节点获取 -----> 与远程节点交互 --> 返回缓存值 ⑵
                            |  否
                            |-----> 调用`回调函数`，获取值并添加到缓存 --> 返回缓存值 ⑶
*/
// 提供被其他节点访问的能力

// Getter 负责在缓存未命中时回源加载数据。
// Getter 内可以调用 Get 加载其他 key，但不能在加载 key K 的过程中（同一
// goroutine 内）再次调用 Get(K)：这会形成等待自身的循环，此时会立即返回
// ErrReentrantLoad。跨 goroutine 的循环依赖无法自动识别，仍需调用方避免。
type Getter interface {
	// []byte 想一下byteview, 二进制
	Get(key string) ([]byte, error)
}

// ErrReentrantLoad 表示 Getter 在加载某个 key 时，又通过 Get 加载同一个 key。
// 这种循环等待无法解开，因此立即返回错误而不是死锁。
var ErrReentrantLoad = singleflight.ErrReentrant

// Go 函数类型 + 接口，**函数可以实现接口**，这个设计叫【函数适配器】
// 定义一个函数类型
type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// group是独立缓存命名空间
type Group struct {
	name      string
	getter    Getter     // **找不到缓存的时候，去哪里加载原始数据**」，**不是用来读缓存的！**
	mainCache cache      // 本机内存缓存， 读LRU缓存（之前带sync.Mutex封装的缓存层）
	peers     PeerPicker //远程节点选择器
	//用来合并并发请求
	loader *singleflight.Group
	// DefaultTTL 默认过期时间，0 表示永不过期
	DefaultTTL time.Duration
	// breaker 远程节点熔断器，失败 N 次后跳过远程直接降级到本地
	breaker *CircuitBreaker
	metrics *Metrics
}

// Metrics 返回当前 Group 的指标快照
func (g *Group) Metrics() MetricsSnapshot {
	return g.metrics.Snapshot()
}

// CacheBytes 返回当前 Group 本地缓存的实时字节占用。
// 该值在调用时从 LRU 读取，而不是单独维护的计数器，因此不会与真实内存用量漂移。
func (g *Group) CacheBytes() int64 {
	return g.mainCache.Bytes()
}

func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

var (
	mu sync.RWMutex
	// 多个group是多套独立的缓存命名空间
	//PeerPicker解决的是同一个Group内， 多机器分布式
	//整个集群的所有服务器 = 一个大快递驿站，有**多个快递货架区**，每个货架区就是一个 **Group**
	//PeerPicker 是**同一个货架区内部的分拣员**
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	// cacheBytes 必须 >= 0：lru.Unlimited(0) 表示不限制内存，负数属于非法配置。
	// 在构造期就 panic，避免配置错误让缓存静默失效或变成无界缓存。
	if cacheBytes < 0 {
		panic(fmt.Sprintf("geecache: cacheBytes for group %q must be >= 0 (0 means unlimited), got %d", name, cacheBytes))
	}
	mu.Lock()
	defer mu.Unlock()
	m := NewMetrics()
	g := &Group{
		name:    name,
		getter:  getter,
		metrics: m,
		mainCache: cache{
			cacheBytes: cacheBytes,
			metrics:    m,
		},
		loader:  singleflight.NewGroup(),
		breaker: NewCircuitBreaker(3, 10*time.Second), // 连续失败3次 → 熔断10秒
	}
	groups[name] = g
	return g
}
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// 整个GeeCache,别人想查缓存， 统一调用这个方法
// 输入key, 返回缓存数据ByteVie, 找不到报错
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	//查询本地并发LRU缓存
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		g.metrics.RecordHit()
		return v, nil
	}
	//缓存未命中， 进入加载逻辑
	g.metrics.RecordMiss()
	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			//用一致性hash挑选远程节点
			//Peer拿到的是服务器，
			if peer, ok := g.peers.PickPeer(key); ok {
				//发起HTTP请求从远程节点获取数据
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				// 请求远程节点失败了， 降级
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		//key是本地 / 远程节点请求失败
		return g.getLocal(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// 向远程GeeCache节点发起网络请求，获取缓存
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	// 熔断器打开 → 快速失败，上层 load() 自动降级到 getLocal()
	// ticket 记录获准时的状态代数，迟到/失效的结果不会污染新状态
	ticket, ok := g.breaker.Allow()
	if !ok {
		return ByteView{}, ErrCircuitOpen
	}
	//rpc 请求参数， response参数
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	//rpc调用
	err := peer.Get(req, res)
	if err != nil {
		g.breaker.RecordFailure(ticket) // 失败计数+1
		return ByteView{}, err
	}

	g.breaker.RecordSuccess(ticket) // 成功 → 重置熔断器
	g.metrics.RecordPeerLoad()
	//包装成ByteView
	return ByteView{b: res.Value}, nil
}
func (g *Group) getLocal(key string) (ByteView, error) {
	g.metrics.RecordLocalLoad()
	//使用业务回调拉取原始数据
	//拿到的是[]byte
	bytes, err := g.getter.Get(key)
	if err != nil {
		//error handle
		return ByteView{}, err
	}
	//封装为只读ByteView, 深拷贝防止外部篡改
	value := ByteView{
		//深拷贝
		b: cloneBytes(bytes),
	}
	g.populateCache(key, value)
	return value, nil
}

// 写入本地缓存
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value, g.DefaultTTL)
}

// SetTTL 设置默认过期时间并启动后台清理
// cleanupInterval 为 0 时不启动后台清理（仅依赖 Get 时的惰性删除）
func (g *Group) SetTTL(ttl time.Duration, cleanupInterval time.Duration) {
	g.DefaultTTL = ttl
	if cleanupInterval > 0 {
		g.mainCache.StartCleanup(cleanupInterval)
	}
}

//cc
