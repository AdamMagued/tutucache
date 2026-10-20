package main

import (
	"context"
	"flag"
	"fmt"
	"geecache"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	peersFile = "peers.json"
	apiURL    = "http://localhost:9999"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup() *geecache.Group {
	// 最大内存两KB
	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

// listenAddrFromURL 从节点对外 URL 中提取监听用的 host:port。
// 监听地址与 scheme 无关：http:// 和 https:// 绑定的是同一个 host:port，
// 因此不能按 "http://" 的固定长度截取。scheme 只影响 peer 拨号时使用的 URL。
func listenAddrFromURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("parse %q: missing host (want scheme://host:port)", rawURL)
	}
	if _, _, err := net.SplitHostPort(u.Host); err != nil {
		return "", fmt.Errorf("parse %q: %w", rawURL, err)
	}
	return u.Host, nil
}

// 启动**集群内部节点服务**（节点之间通信）。
// selfURL 是节点对外公布的地址（peer 用它拨号）；listenAddr 是本机监听的 host:port。
func startCacheServer(selfURL, listenAddr string, gee *geecache.Group) (*http.Server, *geecache.HTTPPool) {
	pool := geecache.NewHTTPPool(selfURL)

	// 共享 Token 认证（环境变量 GEECACHE_TOKEN）
	if token := os.Getenv("GEECACHE_TOKEN"); token != "" {
		pool.SetSharedToken(token)
		log.Println("shared token auth enabled")
	}

	// TLS 加密（环境变量 GEECACHE_CERT / GEECACHE_KEY / GEECACHE_CA）
	certFile := os.Getenv("GEECACHE_CERT")
	keyFile := os.Getenv("GEECACHE_KEY")
	if certFile != "" && keyFile != "" {
		caFile := os.Getenv("GEECACHE_CA")
		if err := pool.EnableTLS(certFile, keyFile, caFile); err != nil {
			log.Fatalf("TLS setup failed: %v", err)
		}
		log.Println("TLS enabled")
	}

	// 服务发现
	discovery := geecache.NewFileDiscovery(selfURL, peersFile)
	// 给HTTPPool绑定服务发现
	if err := pool.StartDiscovery(discovery); err != nil {
		log.Fatalf("discovery register failed: %v", err)
	}
	// 给Group注册 [节点选择器PeerPicker]
	gee.RegisterPeers(pool)

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: pool,
	}
	//在后台异步启动缓存节点的 HTTP 服务
	go func() {
		log.Printf("geecache is running at %s", listenAddr)
		var err error
		if pool.TLSConfig() != nil {
			err = srv.ListenAndServeTLS(certFile, keyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			log.Fatalf("geecache server error: %v", err)
		}
		log.Printf("geecache server %s stopped", listenAddr)
	}()

	return srv, pool
}

// startAPIServer 启动前端 API 服务；listenAddr 是本机监听的 host:port。
func startAPIServer(listenAddr string, gee *geecache.Group) *http.Server {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := gee.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())
		}))
	srv := &http.Server{
		Addr:    listenAddr,
		Handler: nil, // 使用 DefaultServeMux
	}
	//新开 goroutine 异步启动 API 服务：
	go func() {
		log.Printf("frontend server is running at %s", listenAddr)
		//`ListenAndServe()` 阻塞，监听 9999 端口，接收用户 http 请求
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("API server error: %v", err)
		}
		log.Printf("frontend server %s stopped", listenAddr)
	}()

	return srv
}

func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()
	/*

	   # port=8001，api=false（不启动9999前端API）
	   go run main.go -port 8001

	   # port=8001，api=true（启动9999前端API服务）
	   go run main.go -port 8001 -api

	*/
	// 每个端口对应一个地址
	// 端口 -> 节点完整地址映射
	portToAddr := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}
	selfURL := portToAddr[port]
	if selfURL == "" {
		log.Fatalf("unsupported port: %d (use 8001/8002/8003)", port)
	}
	// 监听地址从对外 URL 中解析出来：与 scheme 无关，http/https 都监听同一个 host:port。
	listenAddr, err := listenAddrFromURL(selfURL)
	if err != nil {
		log.Fatalf("invalid cache node address: %v", err)
	}

	//创建缓存 Group 实例
	gee := createGroup()

	// 启动服务
	var apiSrv *http.Server
	if api {
		apiListenAddr, err := listenAddrFromURL(apiURL)
		if err != nil {
			log.Fatalf("invalid API address: %v", err)
		}
		apiSrv = startAPIServer(apiListenAddr, gee)
	}
	//启动**集群内部通信缓存节点服务**
	cacheSrv, _ := startCacheServer(selfURL, listenAddr, gee)

	// =========================================================================
	// 等待退出信号，执行优雅关闭
	// =========================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %s, shutting down...", sig)
	//收到信号后才走ctx这里，然后优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if apiSrv != nil {
		if err := apiSrv.Shutdown(ctx); err != nil {
			log.Printf("API server shutdown error: %v", err)
		}
	}
	if err := cacheSrv.Shutdown(ctx); err != nil {
		log.Printf("cache server shutdown error: %v", err)
	}
	// 停止 Group 的后台清理循环（若通过 SetTTL 启动过）。
	gee.Close()

	log.Println("all servers stopped gracefully")
}
