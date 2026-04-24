package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/user-for-download/go-dota/internal/models"
	goredis "github.com/redis/go-redis/v9"
)

const (
	parseQueueKey        = "parse_queue"
	failedTasksQueueKey = "failed_queue"
	permanentDLQKey    = "permanent_failed_queue"
	rawDataKeyPrefix   = "raw_data:"
	fetchQueueKey     = "fetch_queue"
	seenSetFetchKey    = "seen_fetch_ids"
	seenSetParseKey   = "seen_parse_ids"
	retryCountPrefix   = "retry_count:"
)

const (
	rawDataTTL        = 7200 * time.Second
	seenSetFetchTTL   = 86400 * time.Second
	seenSetParseTTL  = 86400 * time.Second
	retryCountTTL    = 86400 * time.Second
)

func (c *Client) PushFetchTask(ctx context.Context, task models.FetchTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	if err := c.rdb.RPush(ctx, fetchQueueKey, data).Err(); err != nil {
		return fmt.Errorf("rpush fetch_queue: %w", err)
	}
	return nil
}

func (c *Client) PopFetchTask(ctx context.Context) (models.FetchTask, error) {
	result, err := c.rdb.BLPop(ctx, 5*time.Second, fetchQueueKey).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return models.FetchTask{}, fmt.Errorf("no task in queue")
		}
		return models.FetchTask{}, fmt.Errorf("blpop fetch_queue: %w", err)
	}
	if len(result) < 2 {
		return models.FetchTask{}, fmt.Errorf("unexpected result length")
	}
	var task models.FetchTask
	if err := json.Unmarshal([]byte(result[1]), &task); err != nil {
		return models.FetchTask{}, fmt.Errorf("unmarshal task: %w", err)
	}
	return task, nil
}

func (c *Client) PushParseTask(ctx context.Context, taskID string) error {
	if err := c.rdb.RPush(ctx, parseQueueKey, taskID).Err(); err != nil {
		return fmt.Errorf("rpush parse_queue: %w", err)
	}
	return nil
}

func (c *Client) PopParseTask(ctx context.Context) (string, error) {
	result, err := c.rdb.BLPop(ctx, 5*time.Second, parseQueueKey).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return "", fmt.Errorf("no task in queue")
		}
		return "", fmt.Errorf("blpop parse_queue: %w", err)
	}
	if len(result) < 2 {
		return "", fmt.Errorf("unexpected result length")
	}
	return result[1], nil
}

func (c *Client) StoreRawData(ctx context.Context, taskID string, data []byte) error {
	key := rawDataKeyPrefix + taskID
	if err := c.rdb.Set(ctx, key, data, rawDataTTL).Err(); err != nil {
		return fmt.Errorf("set raw_data:%s: %w", taskID, err)
	}
	return nil
}

var enqueueRawDataScript = goredis.NewScript(`
	local rawKey = KEYS[1]
	local parseKey = KEYS[2]
	local taskID = ARGV[1]
	local data = ARGV[2]
	local ttl = tonumber(ARGV[3])

	redis.call("SET", rawKey, data, "EX", ttl)
	redis.call("RPUSH", parseKey, taskID)
	return 1
`)

func (c *Client) AtomicEnqueueRawData(ctx context.Context, taskID string, data []byte) error {
	rawKey := rawDataKeyPrefix + taskID
	ttlSeconds := int(rawDataTTL.Seconds())

	_, err := enqueueRawDataScript.Run(ctx, c.rdb, []string{rawKey, parseQueueKey}, taskID, string(data), ttlSeconds).Result()
	if err != nil {
		return fmt.Errorf("atomic enqueue: %w", err)
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
	return json.RawMessage(result), nil
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
	if err := c.rdb.Expire(ctx, key, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("extend raw_data ttl %s: %w", taskID, err)
	}
	return nil
}

func (c *Client) PushFailedTask(ctx context.Context, taskID string) error {
	if err := c.rdb.LPush(ctx, failedTasksQueueKey, taskID).Err(); err != nil {
		return fmt.Errorf("lpush failed_tasks_queue: %w", err)
	}
	return nil
}

func (c *Client) PushPermanentFailedTask(ctx context.Context, taskID string) error {
	if err := c.rdb.RPush(ctx, permanentDLQKey, taskID).Err(); err != nil {
		return fmt.Errorf("rpush permanent_failed_queue: %w", err)
	}
	return nil
}

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

func (c *Client) RequeueFailedTasks(ctx context.Context) (int64, error) {
	return c.RequeueFailedTasksBatch(ctx, 100)
}

// RequeueFailedTasksBatch requeues failed tasks (LIFO order — newest failures first).
// Poison pills (retries >= maxRetryCount) are moved to permanent_failed_queue via RPush.
func (c *Client) RequeueFailedTasksBatch(ctx context.Context, batchSize int) (int64, error) {
	var count int64
	for i := 0; i < batchSize; i++ {
		taskID, err := c.rdb.LPop(ctx, failedTasksQueueKey).Result()
		if errors.Is(err, goredis.Nil) {
			break
		} else if err != nil {
			return count, err
		}

		retryCount, _ := c.GetRetryCount(ctx, taskID)
		if retryCount >= int64(c.cfg.MaxRetryCount) {
			_ = c.rdb.RPush(ctx, permanentDLQKey, taskID).Err()
			continue
		}

		if err := c.PushParseTask(ctx, taskID); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (c *Client) MarkFetchIDSeen(ctx context.Context, id string) error {
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, seenSetFetchKey, id)
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

func (c *Client) GetQueueLen(ctx context.Context) (int64, error) {
	return c.rdb.LLen(ctx, fetchQueueKey).Result()
}