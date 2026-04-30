package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/pipeline"
)

func (c *Client) EnsureStreamGroups(ctx context.Context) error {
	groups := []struct {
		stream string
		group  string
	}{
		{pipeline.FetchTasksStream, pipeline.CollectorGroup},
		{pipeline.ParseTasksStream, pipeline.ParserGroup},
	}

	for _, g := range groups {
		err := c.rdb.XGroupCreateMkStream(ctx, g.stream, g.group, "0").Err()
		if err != nil && !IsXGroupAlreadyExists(err) {
			return fmt.Errorf("create group %s on %s: %w", g.group, g.stream, err)
		}
	}
	return nil
}

func (c *Client) AddFetchStreamTask(ctx context.Context, task models.FetchStreamTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal fetch task: %w", err)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: pipeline.FetchTasksStream,
		MaxLen: pipeline.StreamMaxStreamSize,
		Approx: true,
		Values: map[string]interface{}{"data": string(data)},
	}).Err()
}

func (c *Client) ReaddFetchStreamTask(ctx context.Context, task models.FetchStreamTask, retryCount int) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal fetch task: %w", err)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: pipeline.FetchTasksStream,
		MaxLen: pipeline.StreamMaxStreamSize,
		Approx: true,
		Values: map[string]interface{}{"data": string(data), "retry_count": retryCount},
	}).Err()
}

func (c *Client) AddFetchToDLQ(ctx context.Context, task models.FetchStreamTask, reason string, retryCount int) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal fetch task: %w", err)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: pipeline.FetchDLQStream,
		MaxLen: pipeline.StreamMaxStreamSize,
		Approx: true,
		Values: map[string]interface{}{"data": string(data), "reason": reason, "retry_count": retryCount},
	}).Err()
}

func (c *Client) ReadFetchTasks(ctx context.Context, group, consumer string, count int64, blockMS int) ([]models.StreamMessage, error) {
	streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{pipeline.FetchTasksStream, ">"},
		Count:    count,
		Block:    time.Duration(blockMS) * time.Millisecond,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup fetch: %w", err)
	}

	return parseStreamMessages(streams)
}

func (c *Client) AddParseStreamTask(ctx context.Context, task models.ParseStreamTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal parse task: %w", err)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: pipeline.ParseTasksStream,
		MaxLen: pipeline.StreamMaxStreamSize,
		Approx: true,
		Values: map[string]interface{}{"data": string(data)},
	}).Err()
}

func (c *Client) ReaddParseStreamTask(ctx context.Context, task models.ParseStreamTask, retryCount int) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal parse task: %w", err)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: pipeline.ParseTasksStream,
		MaxLen: pipeline.StreamMaxStreamSize,
		Approx: true,
		Values: map[string]interface{}{"data": string(data), "retry_count": retryCount},
	}).Err()
}

func (c *Client) AddParseToDLQ(ctx context.Context, task models.ParseStreamTask, reason string, retryCount int) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal parse task: %w", err)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: pipeline.ParseDLQStream,
		MaxLen: pipeline.StreamMaxStreamSize,
		Approx: true,
		Values: map[string]interface{}{"data": string(data), "reason": reason, "retry_count": retryCount},
	}).Err()
}

func (c *Client) ReadParseTasks(ctx context.Context, group, consumer string, count int64, blockMS int) ([]models.StreamMessage, error) {
	streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{pipeline.ParseTasksStream, ">"},
		Count:    count,
		Block:    time.Duration(blockMS) * time.Millisecond,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup parse: %w", err)
	}

	return parseStreamMessages(streams)
}

func (c *Client) AckFetchTask(ctx context.Context, stream, id string) error {
	return c.rdb.XAck(ctx, stream, pipeline.CollectorGroup, id).Err()
}

func (c *Client) AckParseTask(ctx context.Context, stream, id string) error {
	return c.rdb.XAck(ctx, stream, pipeline.ParserGroup, id).Err()
}

func parseStreamMessages(results []redis.XStream) ([]models.StreamMessage, error) {
	var msgs []models.StreamMessage
	for _, stream := range results {
		for _, msg := range stream.Messages {
			data, ok := msg.Values["data"].(string)
			if !ok {
				continue
			}

			var task interface{}
			if err := json.Unmarshal([]byte(data), &task); err != nil {
				continue
			}

			retryCount := 0
			if rc, ok := msg.Values["retry_count"].(string); ok {
				fmt.Sscanf(rc, "%d", &retryCount)
			}

			msgs = append(msgs, models.StreamMessage{
				ID:         msg.ID,
				Task:       task,
				RetryCount: retryCount,
			})
		}
	}
	return msgs, nil
}

func IsXGroupAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func (c *Client) ClaimStaleFetchTasks(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]models.StreamMessage, error) {
	return c.claimStaleTasks(ctx, pipeline.FetchTasksStream, pipeline.CollectorGroup, consumer, minIdle, count)
}

func (c *Client) ClaimStaleParseTasks(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]models.StreamMessage, error) {
	return c.claimStaleTasks(ctx, pipeline.ParseTasksStream, pipeline.ParserGroup, consumer, minIdle, count)
}

func (c *Client) claimStaleTasks(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]models.StreamMessage, error) {
	messages, _, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Count:    count,
		Start:    "0-0",
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xautoclaim %s %s: %w", stream, group, err)
	}

	if len(messages) == 0 {
		return nil, nil
	}

	return parseStreamMessages([]redis.XStream{{Messages: messages}})
}
