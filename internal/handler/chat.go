package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"enterprise-ai-gateway/internal/cache"
	"enterprise-ai-gateway/internal/limiter"
	"enterprise-ai-gateway/internal/proxy"
	"enterprise-ai-gateway/pkg/tokenizer"
)

const maxRequestBodyBytes = 4 << 20 // 4 MiB

// ChatHandler applies token-aware rate limiting and semantic caching before proxying upstream.
type ChatHandler struct {
	Limiter *limiter.TokenLimiter
	Cache   *cache.SemanticCache
	Proxy   *httputil.ReverseProxy
}

type chatCompletionRequest struct {
	Messages []chatMessage `json:"messages"`
}

type chatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type rateLimitErrorResponse struct {
	Error rateLimitError `json:"error"`
}

type rateLimitError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

// ServeHTTP handles POST /v1/chat/completions.
func (h *ChatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenantID := tenantFromAuth(r.Header.Get("Authorization"))

	body, err := readAndRestoreBody(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	promptText, err := extractPromptText(body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	tokens, err := tokenizer.EstimateTokens(promptText)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to estimate tokens")
		return
	}

	allowed, err := h.Limiter.Allow(ctx, tenantID, tokens)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "rate limiter unavailable")
		return
	}
	if !allowed {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(rateLimitErrorResponse{
			Error: rateLimitError{
				Message: fmt.Sprintf("Rate limit exceeded for tenant %q", tenantID),
				Type:    "rate_limit_error",
				Code:    "rate_limit_exceeded",
			},
		})
		return
	}

	ctxHash, userPrompt, err := extractCacheContext(body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	var promptVector []float32
	if userPrompt != "" && h.Cache != nil {
		vector, err := h.Cache.GenerateEmbedding(ctx, userPrompt)
		if err != nil {
			log.Printf("cache: embedding failed, proxying: %v", err)
		} else {
			promptVector = vector
			cached, err := h.Cache.Search(ctx, tenantID, ctxHash, vector, 0.95)
			if err == nil {
				writeCachedCompletion(w, cached)
				return
			}
			if !errors.Is(err, cache.ErrCacheMiss) {
				log.Printf("cache: search failed, proxying: %v", err)
			}
		}
	}

	interceptor := proxy.NewStreamInterceptor(w)
	h.Proxy.ServeHTTP(interceptor, r)

	if h.Cache != nil && len(promptVector) > 0 {
		responseText := interceptor.CapturedText()
		status := interceptor.StatusCode()
		switch {
		case responseText == "":
			log.Printf("[Handler Cache Save Skipped]: empty captured text (status=%d)", status)
		case status != http.StatusOK:
			log.Printf("[Handler Cache Save Skipped]: upstream status=%d", status)
		default:
			log.Printf("[Handler Sync Save Trigger]: tenant=%s textLen=%d vectorDim=%d", tenantID, len(responseText), len(promptVector))
			saveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := h.Cache.Save(saveCtx, tenantID, ctxHash, userPrompt, responseText, promptVector); err != nil {
				log.Printf("[Handler Sync Save Error]: %v", err)
			} else {
				log.Printf("[Handler Sync Save Success]")
			}
		}
	}
}

func tenantFromAuth(authHeader string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(authHeader, prefix) {
		return "default-tenant"
	}
	tenant := strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
	if tenant == "" {
		return "default-tenant"
	}
	return tenant
}

func readAndRestoreBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()

	limited := io.LimitReader(r.Body, maxRequestBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(body) > maxRequestBodyBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxRequestBodyBytes)
	}

	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	return body, nil
}

func extractPromptText(body []byte) (string, error) {
	if len(body) == 0 {
		return "", nil
	}

	var req chatCompletionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", fmt.Errorf("parse chat completion payload: %w", err)
	}

	var parts []string
	for _, msg := range req.Messages {
		text := messageContentText(msg.Content)
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n"), nil
}

func extractCacheContext(body []byte) (ctxHash string, userPrompt string, err error) {
	if len(body) == 0 {
		return "no-ctx", "", nil
	}

	var req chatCompletionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", "", fmt.Errorf("parse chat completion payload: %w", err)
	}

	var systemParts []string
	var userParts []string
	for _, msg := range req.Messages {
		text := messageContentText(msg.Content)
		if text == "" {
			continue
		}
		switch msg.Role {
		case "system":
			systemParts = append(systemParts, text)
		case "user":
			userParts = append(userParts, text)
		}
	}

	if len(systemParts) == 0 {
		ctxHash = "no-ctx"
	} else {
		sum := sha256.Sum256([]byte(strings.Join(systemParts, "\n")))
		ctxHash = hex.EncodeToString(sum[:])
	}

	return ctxHash, strings.Join(userParts, "\n"), nil
}

func writeCachedCompletion(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "HIT")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":      "chatcmpl-cached",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   "semantic-cache",
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": content,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]int{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
	})
}

func messageContentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}

	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var sb strings.Builder
		for _, b := range blocks {
			if b.Type != "text" || b.Text == "" {
				continue
			}
			if sb.Len() > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(b.Text)
		}
		return sb.String()
	}

	return ""
}

func writeJSONError(w http.ResponseWriter, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    errType,
		},
	})
}
