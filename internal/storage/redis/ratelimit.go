package redis

import (
	"context"
	"fmt"
	"net/url"

	goredis "github.com/redis/go-redis/v9"
)

var rateLimitScript = goredis.NewScript(`
	local min_key = KEYS[1]
	local day_key = KEYS[2]
	local max_min = tonumber(ARGV[1])
	local max_day = tonumber(ARGV[2])
	local pool_key = ARGV[3]
	local proxy = ARGV[4]

	local current_min = tonumber(redis.call("GET", min_key) or "0")
	local current_day = tonumber(redis.call("GET", day_key) or "0")

	if (max_min > 0 and current_min >= max_min) or (max_day > 0 and current_day >= max_day) then
		if max_day > 0 and current_day >= max_day then
			redis.call("SREM", pool_key, proxy)
		end
		return 0
	end

	local new_min = redis.call("INCR", min_key)
	if new_min == 1 then redis.call("EXPIRE", min_key, 60) end

	local new_day = redis.call("INCR", day_key)
	if new_day == 1 then redis.call("EXPIRE", day_key, 86400) end

	return 1
`)

func extractIP(proxyURL string) string {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return proxyURL
	}
	return u.Hostname()
}

func (c *Client) AtomicRateLimit(ctx context.Context, proxyURL string) (bool, error) {
	ip := extractIP(proxyURL)
	minKey := fmt.Sprintf("proxy_req_min:%s", ip)
	dayKey := fmt.Sprintf("proxy_req_day:%s", ip)

	result, err := rateLimitScript.Run(ctx, c.rdb, []string{minKey, dayKey}, c.cfg.MaxReqPerMin, c.cfg.MaxReqPerDay, proxyPoolKey, proxyURL).Result()
	if err != nil {
		return false, fmt.Errorf("lua script error: %w", err)
	}

	return result.(int64) == 1, nil
}