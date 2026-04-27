package redis

import (
	"context"
	"fmt"
	"math/rand"

	goredis "github.com/redis/go-redis/v9"
)

const proxyPoolKey = "proxy_pool"
const proxyRankingKey = "proxy_ranking"
const proxyFailurePrefix = "proxy_failures:"

func proxyFailureKey(proxy string) string {
	return proxyFailurePrefix + proxy
}

var recordSuccessScript = goredis.NewScript(`
	local rankingKey = KEYS[1]
	local failKey = KEYS[2]
	local proxy = ARGV[1]
	local current = redis.call("ZSCORE", rankingKey, proxy)
	local score = 1
	if current then
		score = math.min(10000, math.max(1, tonumber(current) + 1))
	end
	redis.call("ZADD", rankingKey, score, proxy)
	redis.call("DEL", failKey)
	return 1
`)

var recordFailureScript = goredis.NewScript(`
	local rankingKey = KEYS[1]
	local failKey = KEYS[2]
	local proxy = ARGV[1]
	local maxFails = tonumber(ARGV[2])
	local failCount = redis.call("INCR", failKey)
	if failCount >= maxFails then
		redis.call("ZADD", rankingKey, -1000, proxy)
		redis.call("DEL", failKey)
	end
	return failCount
`)

// DefaultMaxProxyFails is the default maximum consecutive failures before a proxy is penalized.
const DefaultMaxProxyFails = 3

var weightedRandomProxyScript = goredis.NewScript(`
	-- weightedRandomProxyScript selects a proxy using weighted random sampling.
	-- The +10 baseline ensures even heavily-penalized proxies (score < 0) still get
	-- some selection weight, allowing them to be retried occasionally.
	local poolKey = KEYS[1]
	local rankingKey = KEYS[2]
	local failurePrefix = ARGV[1]
	local penalty = tonumber(ARGV[2])
	local fraction = tonumber(ARGV[3])

	local proxies = redis.call("SMEMBERS", poolKey)
	if #proxies == 0 then
		return {}
	end

	local total = 0
	local scored = {}
	for i, proxy in ipairs(proxies) do
		local score = redis.call("ZSCORE", rankingKey, proxy)
		if not score then
			score = 0
		else
			score = tonumber(score)
			if score > 10000 then score = 10000 elseif score < 0 then score = 0 end
		end
		local failKey = failurePrefix .. proxy
		local fails = redis.call("GET", failKey)
		if fails then
			score = math.max(0, score - tonumber(fails) * penalty)
		end
		score = math.max(0, math.min(10000, score + 10))
		total = total + score
		scored[#scored + 1] = {proxy, score}
	end

	local threshold = fraction * total
	local cumulative = 0
	for i, entry in ipairs(scored) do
		cumulative = cumulative + entry[2]
		if cumulative >= threshold then
			return {entry[1], tostring(total)}
		end
	end
	if #scored > 0 then
		return {scored[#scored][1], tostring(total)}
	end
	return {}
`)

func (c *Client) AddProxies(ctx context.Context, proxies []string) error {
	if len(proxies) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	pipe.Del(ctx, proxyPoolKey)
	pipe.Del(ctx, proxyRankingKey)
	for _, p := range proxies {
		pipe.SAdd(ctx, proxyPoolKey, p)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("add proxies: %w", err)
	}
	return nil
}

func (c *Client) RecordProxySuccess(ctx context.Context, proxy string) error {
	failKey := proxyFailureKey(proxy)
	_, err := recordSuccessScript.Run(ctx, c.rdb, []string{proxyRankingKey, failKey}, proxy).Result()
	if err != nil {
		return fmt.Errorf("record proxy success: %w", err)
	}
	return nil
}

func (c *Client) RecordProxyFailure(ctx context.Context, proxy string, maxFails int) error {
	failKey := proxyFailureKey(proxy)
	_, err := recordFailureScript.Run(ctx, c.rdb, []string{proxyRankingKey, failKey}, proxy, maxFails).Result()
	if err != nil {
		return fmt.Errorf("record proxy failure: %w", err)
	}
	return nil
}

func (c *Client) GetWeightedRandomProxy(ctx context.Context) (string, error) {
	fraction := rand.Float64()

	result, err := weightedRandomProxyScript.Run(
		ctx, c.rdb,
		[]string{proxyPoolKey, proxyRankingKey},
		proxyFailurePrefix, 10, fraction,
	).Result()
	if err != nil {
		return "", fmt.Errorf("weighted random proxy: %w", err)
	}

	parts, ok := result.([]interface{})
	if !ok || len(parts) == 0 {
		return "", fmt.Errorf("no proxies available in pool")
	}

	proxy, ok := parts[0].(string)
	if !ok || proxy == "" {
		return "", fmt.Errorf("no proxies available in pool")
	}
	return proxy, nil
}

func (c *Client) GetProxyCount(ctx context.Context) (int64, error) {
	count, err := c.rdb.SCard(ctx, proxyPoolKey).Result()
	if err != nil {
		return 0, fmt.Errorf("scard: %w", err)
	}
	return count, nil
}

func (c *Client) RemoveProxy(ctx context.Context, proxy string) error {
	if err := c.rdb.SRem(ctx, proxyPoolKey, proxy).Err(); err != nil {
		return fmt.Errorf("srem proxy: %w", err)
	}
	return nil
}