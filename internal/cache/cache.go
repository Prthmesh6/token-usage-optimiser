package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	indexName      = "cache_idx"
	embeddingModel = "llama3"
	cacheTTL       = 24 * time.Hour
)

// ErrCacheMiss indicates no semantically similar cached response was found.
var ErrCacheMiss = errors.New("cache miss")

// SemanticCache stores and retrieves vector-indexed prompt/response pairs in Redis.
type SemanticCache struct {
	rdb        redis.UniversalClient
	ollamaURL  string
	httpClient *http.Client
	vectorDim  int
}

// NewSemanticCache returns a cache backed by Redis and Ollama embeddings.
func NewSemanticCache(rdb redis.UniversalClient, ollamaURL string) *SemanticCache {
	return &SemanticCache{
		rdb:       rdb,
		ollamaURL: strings.TrimRight(ollamaURL, "/"),
		httpClient: &http.Client{
			Timeout: 0, // per-request deadlines come from ctx
		},
	}
}

// InitIndex probes Ollama for the embedding dimension, then creates (or rebuilds) the vector index.
func (c *SemanticCache) InitIndex(ctx context.Context) error {
	probe, err := c.probeEmbedding(ctx)
	if err != nil {
		return err
	}
	c.vectorDim = len(probe)
	log.Printf("[Cache] embedding model %q produces %d dimensions", embeddingModel, c.vectorDim)

	err = c.createIndex(ctx, c.vectorDim)
	if err == nil {
		return nil
	}
	if !isIndexAlreadyExists(err) {
		return err
	}

	indexedDim, dimErr := c.indexVectorDimension(ctx)
	if dimErr != nil {
		log.Printf("[Cache] could not read index dimension, continuing: %v", dimErr)
		return nil
	}
	if indexedDim == c.vectorDim {
		log.Printf("[Cache] index %q already exists with dim=%d", indexName, indexedDim)
		return nil
	}

	log.Printf("[Cache] rebuilding index %q: index dim=%d model dim=%d", indexName, indexedDim, c.vectorDim)
	if err := c.rdb.Do(ctx, "FT.DROPINDEX", indexName, "DD").Err(); err != nil {
		return fmt.Errorf("cache: drop index: %w", err)
	}
	return c.createIndex(ctx, c.vectorDim)
}

// probeEmbedding retries while Ollama cold-loads the model (first embed can take minutes).
func (c *SemanticCache) probeEmbedding(ctx context.Context) ([]float32, error) {
	const maxAttempts = 6
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("cache: probe embedding: %w", err)
		}

		vec, err := c.GenerateEmbedding(ctx, "dimension probe")
		if err == nil {
			if attempt > 1 {
				log.Printf("[Cache] probe succeeded on attempt %d", attempt)
			}
			return vec, nil
		}

		lastErr = err
		log.Printf("[Cache] probe attempt %d/%d failed: %v", attempt, maxAttempts, err)
		if attempt == maxAttempts {
			break
		}

		backoff := time.Duration(attempt) * 5 * time.Second
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("cache: probe embedding: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}

	return nil, fmt.Errorf(
		"cache: probe embedding: %w (is Ollama running? try: ollama serve && ollama pull %s)",
		lastErr, embeddingModel,
	)
}

func (c *SemanticCache) createIndex(ctx context.Context, dim int) error {
	err := c.rdb.Do(ctx,
		"FT.CREATE", indexName,
		"ON", "HASH",
		"PREFIX", "1", "cache:",
		"SCHEMA",
		"tenant_id", "TAG",
		"context_hash", "TAG",
		"prompt_raw", "TEXT",
		"response_raw", "TEXT",
		"prompt_vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", strconv.Itoa(dim), "DISTANCE_METRIC", "COSINE",
	).Err()
	if err != nil && !isIndexAlreadyExists(err) {
		return fmt.Errorf("cache: create index: %w", err)
	}
	return nil
}

type embedRequest struct {
	Model  string `json:"model"`
	Input  any    `json:"input,omitempty"`
	Prompt string `json:"prompt,omitempty"`
}

type legacyEmbeddingResponse struct {
	Embedding []float64 `json:"embedding"`
}

type modernEmbeddingResponse struct {
	Embeddings [][]float64 `json:"embeddings"`
}

// GenerateEmbedding calls Ollama (/api/embed, falling back to /api/embeddings) for the given text.
func (c *SemanticCache) GenerateEmbedding(ctx context.Context, text string) ([]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	vector, err := c.requestEmbedding(ctx, c.ollamaURL+"/api/embed", embedRequest{
		Model: embeddingModel,
		Input: text,
	})
	if err == nil {
		return vector, nil
	}

	log.Printf("[Cache] /api/embed failed (%v), trying /api/embeddings", err)
	return c.requestEmbedding(ctx, c.ollamaURL+"/api/embeddings", embedRequest{
		Model:  embeddingModel,
		Prompt: text,
	})
}

func (c *SemanticCache) requestEmbedding(ctx context.Context, url string, body embedRequest) ([]float32, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	values, err := parseEmbeddingResponse(respBody)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("empty embedding in response from %s", url)
	}

	vector := make([]float32, len(values))
	for i, v := range values {
		vector[i] = float32(v)
	}
	if c.vectorDim > 0 && len(vector) != c.vectorDim {
		return nil, fmt.Errorf("embedding dimension %d does not match index dimension %d", len(vector), c.vectorDim)
	}
	return vector, nil
}

func parseEmbeddingResponse(body []byte) ([]float64, error) {
	var modern modernEmbeddingResponse
	if err := json.Unmarshal(body, &modern); err == nil && len(modern.Embeddings) > 0 && len(modern.Embeddings[0]) > 0 {
		return modern.Embeddings[0], nil
	}

	var legacy legacyEmbeddingResponse
	if err := json.Unmarshal(body, &legacy); err == nil && len(legacy.Embedding) > 0 {
		return legacy.Embedding, nil
	}

	return nil, fmt.Errorf("parse embedding response: %s", strings.TrimSpace(string(body)))
}

// Search looks up the nearest cached response for tenant/context. Returns response_raw on hit.
func (c *SemanticCache) Search(ctx context.Context, tenantID, ctxHash string, vector []float32, threshold float64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(vector) == 0 {
		return "", ErrCacheMiss
	}
	if c.vectorDim > 0 && len(vector) != c.vectorDim {
		return "", fmt.Errorf("cache: query vector dim %d != index dim %d", len(vector), c.vectorDim)
	}

	query := fmt.Sprintf(
		"@tenant_id:%s @context_hash:%s=>[KNN 1 @prompt_vector $query_vec AS vector_score]",
		tagQueryValue(tenantID),
		tagQueryValue(ctxHash),
	)
	maxDistance := 1.0 - threshold

	log.Printf("[Cache FT.SEARCH Query]: %s", query)

	raw, err := c.rdb.Do(ctx,
		"FT.SEARCH", indexName, query,
		"PARAMS", "2", "query_vec", float32ToBytes(vector),
		"SORTBY", "vector_score",
		"LIMIT", "0", "1",
		"DIALECT", "2",
		"RETURN", "2", "vector_score", "response_raw",
	).Result()
	if err != nil {
		log.Printf("[Cache FT.SEARCH DB Error]: %v", err)
		return "", fmt.Errorf("cache: ft.search: %w", err)
	}

	response, distance, found, err := parseSearchResult(raw)
	if err != nil {
		log.Printf("[Cache FT.SEARCH Parse Error]: %v", err)
		return "", err
	}
	if !found {
		log.Printf("[Cache FT.SEARCH] 0 results (tenant=%q ctxHash=%q)", tenantID, ctxHash)
		return "", ErrCacheMiss
	}
	if distance > maxDistance {
		log.Printf("[Cache FT.SEARCH] nearest miss: distance=%.6f maxAllowed=%.6f (tenant=%q)", distance, maxDistance, tenantID)
		return "", ErrCacheMiss
	}

	log.Printf("[Cache FT.SEARCH Hit]: distance=%.6f tenant=%q", distance, tenantID)
	return response, nil
}

// Save persists a prompt/response pair and sets a 24-hour TTL.
func (c *SemanticCache) Save(ctx context.Context, tenantID, ctxHash, prompt, response string, vector []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.vectorDim > 0 && len(vector) != c.vectorDim {
		return fmt.Errorf("cache: save vector dim %d != index dim %d", len(vector), c.vectorDim)
	}

	key := fmt.Sprintf("cache:tenant:%s:ctx:%s:msg:%s", tenantID, ctxHash, uuid.New().String())
	if err := c.rdb.HSet(ctx, key,
		"tenant_id", tenantID,
		"context_hash", ctxHash,
		"prompt_raw", prompt,
		"response_raw", response,
		"prompt_vector", float32ToBytes(vector),
	).Err(); err != nil {
		return fmt.Errorf("cache: hset: %w", err)
	}
	if err := c.rdb.Expire(ctx, key, cacheTTL).Err(); err != nil {
		return fmt.Errorf("cache: expire: %w", err)
	}
	log.Printf("[Cache Save Success]: key=%s promptLen=%d responseLen=%d", key, len(prompt), len(response))
	return nil
}

// float32ToBytes encodes a float32 slice as little-endian IEEE 754 bytes for Redis VECTOR fields.
func float32ToBytes(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

func (c *SemanticCache) indexVectorDimension(ctx context.Context) (int, error) {
	raw, err := c.rdb.Do(ctx, "FT.INFO", indexName).Result()
	if err != nil {
		return 0, err
	}
	if dim, ok := findDimInFTInfo(raw); ok {
		return dim, nil
	}
	return 0, fmt.Errorf("dim not found in FT.INFO")
}

func findDimInFTInfo(raw any) (int, bool) {
	switch v := raw.(type) {
	case []any:
		for i := 0; i < len(v)-1; i++ {
			if s, ok := v[i].(string); ok && s == "dim" {
				switch d := v[i+1].(type) {
				case int64:
					return int(d), true
				case int:
					return d, true
				case string:
					n, err := strconv.Atoi(d)
					if err == nil {
						return n, true
					}
				}
			}
		}
		for _, item := range v {
			if dim, ok := findDimInFTInfo(item); ok {
				return dim, true
			}
		}
	}
	return 0, false
}

func isIndexAlreadyExists(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "index already exists") ||
		strings.Contains(msg, "already exists")
}

// tagQueryValue wraps a TAG value for DIALECT 2. Hyphens are literal inside braces;
// escaping them (e.g. my\-tenant) breaks the parser near "=> [KNN ...]".
func tagQueryValue(value string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\', '{', '}':
			b.WriteByte('\\')
			b.WriteByte(value[i])
		default:
			b.WriteByte(value[i])
		}
	}
	b.WriteByte('}')
	return b.String()
}

func parseSearchResult(raw any) (response string, distance float64, found bool, err error) {
	rows, ok := raw.([]any)
	if !ok || len(rows) == 0 {
		return "", 0, false, nil
	}

	total, err := toInt64(rows[0])
	if err != nil {
		return "", 0, false, fmt.Errorf("cache: parse search total: %w", err)
	}
	if total == 0 || len(rows) < 3 {
		return "", 0, false, nil
	}

	fields, ok := rows[2].([]any)
	if !ok {
		return "", 0, false, fmt.Errorf("cache: unexpected search field row type %T", rows[2])
	}

	for i := 0; i+1 < len(fields); i += 2 {
		name, ok := fields[i].(string)
		if !ok {
			continue
		}
		switch name {
		case "vector_score":
			distance, err = toFloat64(fields[i+1])
			if err != nil {
				return "", 0, false, fmt.Errorf("cache: parse vector_score: %w", err)
			}
		case "response_raw":
			response, ok = fields[i+1].(string)
			if !ok {
				return "", 0, false, fmt.Errorf("cache: unexpected response_raw type %T", fields[i+1])
			}
		}
	}

	if response == "" {
		return "", 0, false, nil
	}
	return response, distance, true, nil
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported int type %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case string:
		return strconv.ParseFloat(n, 64)
	default:
		return 0, fmt.Errorf("unsupported float type %T", v)
	}
}
