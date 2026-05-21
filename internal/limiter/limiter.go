package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// allowScript atomically prunes expired ZSET entries, sums in-window token
// weights, and conditionally records a new request.
//
// KEYS[1]  — ratelimit:tenant:{tenantID}:tpm
// ARGV[1]  — window boundary (ms): scores at or below this are expired
// ARGV[2]  — tokensRequested
// ARGV[3]  — tpmLimit
// ARGV[4]  — now (ms), used as the ZADD score
// ARGV[5]  — member: "{uuid}:{tokensRequested}"
//
// Returns 1 if allowed, 0 if rejected.
var allowScript = redis.NewScript(`
local key = KEYS[1]
local window_start = tonumber(ARGV[1])
local tokens_requested = tonumber(ARGV[2])
local tpm_limit = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local member = ARGV[5]

redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

local members = redis.call('ZRANGE', key, 0, -1)
local current_total = 0
for i = 1, #members do
  local sep = string.find(members[i], ':', 1, true)
  if sep then
    local weight = tonumber(string.sub(members[i], sep + 1))
    if weight then
      current_total = current_total + weight
    end
  end
end

if current_total + tokens_requested > tpm_limit then
  return 0
end

redis.call('ZADD', key, now, member)
return 1
`)

// TokenLimiter enforces a per-tenant tokens-per-minute quota using a Redis ZSET sliding window.
type TokenLimiter struct {
	rdb        redis.UniversalClient
	tpmLimit   int
	windowSize time.Duration
}

// NewTokenLimiter returns a limiter backed by the given Redis client.
func NewTokenLimiter(rdb redis.UniversalClient, tpmLimit int, windowSize time.Duration) (*TokenLimiter, error) {
	if rdb == nil {
		return nil, fmt.Errorf("limiter: redis client is nil")
	}
	if tpmLimit <= 0 {
		return nil, fmt.Errorf("limiter: tpmLimit must be positive, got %d", tpmLimit)
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("limiter: windowSize must be positive, got %s", windowSize)
	}
	return &TokenLimiter{
		rdb:        rdb,
		tpmLimit:   tpmLimit,
		windowSize: windowSize,
	}, nil
}

// Allow reports whether tenantID may consume tokensRequested tokens within the sliding window.
// A false result with a nil error means the request was rate-limited.
func (l *TokenLimiter) Allow(ctx context.Context, tenantID string, tokensRequested int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if tenantID == "" {
		return false, fmt.Errorf("limiter: tenantID is required")
	}
	if tokensRequested < 0 {
		return false, fmt.Errorf("limiter: tokensRequested must be non-negative, got %d", tokensRequested)
	}
	if tokensRequested == 0 {
		return true, nil
	}

	now := time.Now().UnixMilli()
	windowStart := now - l.windowSize.Milliseconds()
	key := fmt.Sprintf("ratelimit:tenant:%s:tpm", tenantID)
	member := fmt.Sprintf("%s:%d", uuid.New().String(), tokensRequested)

	result, err := allowScript.Run(
		ctx,
		l.rdb,
		[]string{key},
		windowStart,
		tokensRequested,
		l.tpmLimit,
		now,
		member,
	).Int()
	if err != nil {
		return false, fmt.Errorf("limiter: eval allow script: %w", err)
	}
	return result == 1, nil
}
