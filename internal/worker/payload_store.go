package worker

import (
	"context"
	"encoding/json"

	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type redisPayloadStore struct {
	client *redis.Client
}

func NewRedisPayloadStore(client *redis.Client) PayloadStore {
	return &redisPayloadStore{client: client}
}

func (s *redisPayloadStore) Save(ctx context.Context, taskID string, data []byte) error {
	return s.client.StoreRawData(ctx, taskID, data)
}

func (s *redisPayloadStore) Get(ctx context.Context, taskID string) (json.RawMessage, error) {
	return s.client.GetRawData(ctx, taskID)
}

func (s *redisPayloadStore) Delete(ctx context.Context, taskID string) error {
	return s.client.DeleteRawData(ctx, taskID)
}

func (s *redisPayloadStore) Extend(ctx context.Context, taskID string) error {
	return s.client.ExtendRawDataTTL(ctx, taskID)
}

var _ PayloadStore = (*redisPayloadStore)(nil)