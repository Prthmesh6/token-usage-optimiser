package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"enterprise-ai-gateway/internal/cache"
	"enterprise-ai-gateway/internal/config"
	"enterprise-ai-gateway/internal/handler"
	"enterprise-ai-gateway/internal/limiter"
	"enterprise-ai-gateway/internal/proxy"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTPMLimit   = 10_000
	defaultWindowSize = time.Minute
)

func main() {
	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("redis: %v", err)
	}
	cancel()

	tokenLimiter, err := limiter.NewTokenLimiter(rdb, defaultTPMLimit, defaultWindowSize)
	if err != nil {
		log.Fatalf("limiter: %v", err)
	}

	semanticCache := cache.NewSemanticCache(rdb, cfg.OllamaURL)

	// First Ollama embed loads the model and can take well over 15s on cold start.
	initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	if err := semanticCache.InitIndex(initCtx); err != nil {
		initCancel()
		log.Fatalf("cache index: %v", err)
	}
	initCancel()

	upstream, err := proxy.NewUpstreamProxy(cfg.OllamaURL)
	if err != nil {
		log.Fatalf("proxy: %v", err)
	}

	chatHandler := &handler.ChatHandler{
		Limiter: tokenLimiter,
		Cache:   semanticCache,
		Proxy:   upstream,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.Handle("POST /v1/chat/completions", chatHandler)

	srv := &http.Server{
		Addr:              ":" + cfg.GatewayPort,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout — allow long-lived LLM streaming responses.
	}

	go func() {
		log.Printf("gateway listening on :%s", cfg.GatewayPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
	log.Println("server stopped")
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
