package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	rawDataKeyPrefix = "raw_data:"
	seenSetFetchKey  = "seen_fetch_ids"
	seenSetParseKey  = "seen_parse_ids"
	retryCountPrefix = "retry_count:"
)

var (
	rawDataTTL      = 7200 * time.Second
	seenSetFetchTTL = 86400 * time.Second
	seenSetParseTTL = 86400 * time.Second
	retryCountTTL   = 86400 * time.Second
)

func SetRawDataTTL(seconds int) {
	if seconds > 0 {
		rawDataTTL = time.Duration(seconds) * time.Second
	}
}

// =====================================================================
// Raw Data (Payload) Storage - used by stream-ased workers
// =====================================================================

const maxPayloadBytes = 10 << 20 // 10MB

var ErrPayloadTooLarge = errors.New("payload exceeds maximum size")

func (c *Client) StoreRawData(ctx context.Context, taskID string, data []byte) error {
	if len(data) > maxPayloadBytes {
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, len(data))
	}
	key := rawDataKeyPrefix + taskID
	if err := c.rdb.Set(ctx, key, data, rawDataTTL).Err(); err != nil {
		return fmt.Errorf("set raw_data:%s: %w", taskID, err)
	}
	return nil
}

func (c *Client) GetRawData(ctx context.Context, taskID string) (json.RawMessage, error) {
	key := rawDataKeyPrefix + taskID
	result, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("get raw_data:%s: %w", taskID, err)
	}
	return result, nil
}

func (c *Client) DeleteRawData(ctx context.Context, taskID string) error {
	key := rawDataKeyPrefix + taskID
	if err := c.rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("del raw_data:%s: %w", taskID, err)
	}
	return nil
}

func (c *Client) ExtendRawDataTTL(ctx context.Context, taskID string) error {
	key := rawDataKeyPrefix + taskID
	if err := c.rdb.Expire(ctx, key, rawDataTTL).Err(); err != nil {
		return fmt.Errorf("extend raw_data ttl %s: %w", taskID, err)
	}
	return nil
}

// =====================================================================
// Retry Count Tracking - used by stream-based parser
// =====================================================================

func retryCountKey(taskID string) string {
	return retryCountPrefix + taskID
}

func (c *Client) DeleteRetryCount(ctx context.Context, taskID string) error {
	return c.rdb.Del(ctx, retryCountKey(taskID)).Err()
}

func (c *Client) GetRetryCount(ctx context.Context, taskID string) (int64, error) {
	countStr, err := c.rdb.Get(ctx, retryCountKey(taskID)).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get retry count: %w", err)
	}
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse retry count: %w", err)
	}
	return count, nil
}

func (c *Client) IncrementRetryCount(ctx context.Context, taskID string) error {
	key := retryCountKey(taskID)
	pipe := c.rdb.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, retryCountTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("increment retry count: %w", err)
	}
	return nil
}

// =====================================================================
// Deduplication Sets - used by stream-based workers
// =====================================================================

func (c *Client) MarkFetchIDSeen(ctx context.Context, id string) error {
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, seenSetFetchKey, id)
	pipe.Expire(ctx, seenSetFetchKey, seenSetFetchTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) UnmarkFetchIDSeen(ctx context.Context, id string) error {
	return c.rdb.SRem(ctx, seenSetFetchKey, id).Err()
}

func (c *Client) MarkFetchIDSeenBatch(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	iface := make([]interface{}, len(ids))
	for i, id := range ids {
		iface[i] = id
	}
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, seenSetFetchKey, iface...)
	pipe.Expire(ctx, seenSetFetchKey, seenSetFetchTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) MarkParseIDSeen(ctx context.Context, id string) error {
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, seenSetParseKey, id)
	pipe.Expire(ctx, seenSetParseKey, seenSetParseTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) IsFetchIDSeen(ctx context.Context, id string) (bool, error) {
	return c.rdb.SIsMember(ctx, seenSetFetchKey, id).Result()
}

func (c *Client) IsParseIDSeen(ctx context.Context, id string) (bool, error) {
	return c.rdb.SIsMember(ctx, seenSetParseKey, id).Result()
}
