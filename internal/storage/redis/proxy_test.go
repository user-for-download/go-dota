package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/user-for-download/go-dota/internal/models"
)

func setupRedisContainer(ctx context.Context, t *testing.T) (*Client, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %v", err)
	}

	opts, err := goredis.ParseURL("redis://" + endpoint + "/0")
	if err != nil {
		t.Fatalf("failed to parse redis URL: %v", err)
	}

	rdb := goredis.NewClient(opts)

	cleanup := func() {
		rdb.Close()
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate redis container: %v", err)
		}
	}

	return &Client{rdb: rdb, cfg: ClientConfig{
		MaxRetryCount: 3,
		MaxReqPerMin:  60,
		MaxReqPerDay:  3000,
	}}, cleanup
}

func TestAddProxiesEmpty(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")

	err := client.AddProxies(ctx, nil)
	if err != nil {
		t.Errorf("AddProxies(nil) error = %v", err)
	}

	err = client.AddProxies(ctx, []string{})
	if err != nil {
		t.Errorf("AddProxies([]) error = %v", err)
	}

	client.rdb.Del(ctx, "proxy_pool")
}

func TestAddProxies(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")

	proxies := []string{"socks5://127.0.0.1:1080", "socks5://127.0.0.1:1081"}

	err := client.AddProxies(ctx, proxies)
	if err != nil {
		t.Fatalf("AddProxies() error = %v", err)
	}

	proxy, err := client.GetWeightedRandomProxy(ctx)
	if err != nil {
		t.Fatalf("GetRandomProxy() error = %v", err)
	}
	if proxy == "" {
		t.Error("GetRandomProxy() returned empty string")
	}

	client.rdb.Del(ctx, "proxy_pool")
}

func TestRemoveProxy(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")

	proxies := []string{"socks5://127.0.0.1:1080"}
	client.AddProxies(ctx, proxies)

	err := client.RemoveProxy(ctx, "socks5://127.0.0.1:1080")
	if err != nil {
		t.Fatalf("RemoveProxy() error = %v", err)
	}

	_, err = client.GetWeightedRandomProxy(ctx)
	if err == nil {
		t.Error("GetRandomProxy() should fail after removing proxy")
	}

	client.rdb.Del(ctx, "proxy_pool")
}

func TestGetProxyCount(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")

	client.AddProxies(ctx, []string{"socks5://127.0.0.1:1080"})

	count, err := client.GetProxyCount(ctx)
	if err != nil {
		t.Fatalf("GetProxyCount() error = %v", err)
	}
	if count < 1 {
		t.Errorf("count = %d, want >= 1", count)
	}

	client.rdb.Del(ctx, "proxy_pool")
}

func TestGetWeightedRandomProxyEmpty(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")

	_, err := client.GetWeightedRandomProxy(ctx)
	if err == nil {
		t.Error("GetWeightedRandomProxy() should fail when pool is empty")
	}

	client.rdb.Del(ctx, "proxy_pool")
}

func TestPushFetchTask(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	task := models.FetchTask{MatchID: "12345", URL: "https://example.com/12345"}
	err := client.PushFetchTask(ctx, task)
	if err != nil {
		t.Fatalf("PushFetchTask() error = %v", err)
	}

	popped, err := client.PopFetchTask(ctx)
	if err != nil {
		t.Fatalf("PopFetchTask() error = %v", err)
	}
	if popped.MatchID != task.MatchID {
		t.Errorf("got MatchID %q, want %q", popped.MatchID, task.MatchID)
	}
}

func TestPushParseTask(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	taskID := "test-uuid-001"
	err := client.PushParseTask(ctx, taskID)
	if err != nil {
		t.Fatalf("PushParseTask() error = %v", err)
	}

	poppedID, err := client.PopParseTask(ctx)
	if err != nil {
		t.Fatalf("PopParseTask() error = %v", err)
	}
	if poppedID != taskID {
		t.Errorf("got TaskID %q, want %q", poppedID, taskID)
	}
}

func TestStoreRawData(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	taskID := "test-uuid-raw"
	payload := []byte(`{"player_name":"Dendi"}`)

	err := client.StoreRawData(ctx, taskID, payload)
	if err != nil {
		t.Fatalf("StoreRawData() error = %v", err)
	}

	data, err := client.GetRawData(ctx, taskID)
	if err != nil {
		t.Fatalf("GetRawData() error = %v", err)
	}

	if string(data) != string(payload) {
		t.Errorf("got payload %s, want %s", string(data), string(payload))
	}
}

func TestAtomicRateLimit(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	proxyURL := "socks5://127.0.0.1:9999"

	minKey := "proxy_req_min:127.0.0.1"
	dayKey := "proxy_req_day:127.0.0.1"
	client.rdb.Del(ctx, minKey, dayKey)

	allowed, err := client.AtomicRateLimit(ctx, proxyURL)
	if err != nil {
		t.Fatalf("AtomicRateLimit() error = %v", err)
	}
	if !allowed {
		t.Error("AtomicRateLimit() should allow when no rate limit set")
	}

	minVal, _ := client.rdb.Get(ctx, minKey).Int()
	dayVal, _ := client.rdb.Get(ctx, dayKey).Int()

	if minVal != 1 {
		t.Errorf("min rate = %d, want 1", minVal)
	}
	if dayVal != 1 {
		t.Errorf("day rate = %d, want 1", dayVal)
	}

	client.rdb.Del(ctx, minKey, dayKey)
}

func TestRequeueFailedTasks(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, failedTasksQueueKey)
	client.rdb.Del(ctx, parseQueueKey)

	err := client.PushFailedTask(ctx, "failed-1")
	if err != nil {
		t.Fatalf("PushFailedTask() error = %v", err)
	}
	err = client.PushFailedTask(ctx, "failed-2")
	if err != nil {
		t.Fatalf("PushFailedTask() error = %v", err)
	}

	count, err := client.RequeueFailedTasks(ctx)
	if err != nil {
		t.Fatalf("RequeueFailedTasks() error = %v", err)
	}
	if count != 2 {
		t.Errorf("got count %d, want 2", count)
	}

	queueLen, _ := client.rdb.LLen(ctx, parseQueueKey).Result()
	if queueLen != 2 {
		t.Errorf("parse queue len = %d, want 2", queueLen)
	}
}

func TestWeightedProxySelection(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")
	client.rdb.Del(ctx, "proxy_ranking")

	proxies := []string{"http://proxy-a", "http://proxy-b", "http://proxy-c"}
	err := client.AddProxies(ctx, proxies)
	if err != nil {
		t.Fatalf("AddProxies() error = %v", err)
	}

	err = client.RecordProxySuccess(ctx, "http://proxy-a")
	if err != nil {
		t.Fatalf("RecordProxySuccess() error = %v", err)
	}
	err = client.RecordProxySuccess(ctx, "http://proxy-a")
	if err != nil {
		t.Fatalf("RecordProxySuccess() second call error = %v", err)
	}

	score, err := client.rdb.ZScore(ctx, "proxy_ranking", "http://proxy-a").Result()
	if err != nil {
		t.Fatalf("ZScore() error = %v", err)
	}
	if score != 2 {
		t.Errorf("score = %f, want 2", score)
	}

	proxy, err := client.GetWeightedRandomProxy(ctx)
	if err != nil {
		t.Fatalf("GetWeightedRandomProxy() error = %v", err)
	}
	if proxy == "" {
		t.Error("GetWeightedRandomProxy() returned empty string")
	}

	client.rdb.Del(ctx, "proxy_pool")
	client.rdb.Del(ctx, "proxy_ranking")
}

func TestRecordProxyFailure(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, "proxy_pool")
	client.rdb.Del(ctx, "proxy_ranking")

	err := client.AddProxies(ctx, []string{"http://failing-proxy"})
	if err != nil {
		t.Fatalf("AddProxies() error = %v", err)
	}

	for i := 0; i < 3; i++ {
		err = client.RecordProxyFailure(ctx, "http://failing-proxy", DefaultMaxProxyFails)
		if err != nil {
			t.Fatalf("RecordProxyFailure() attempt %d error = %v", i+1, err)
		}
	}

	score, err := client.rdb.ZScore(ctx, "proxy_ranking", "http://failing-proxy").Result()
	if err != nil {
		t.Fatalf("ZScore() after 3 failures error = %v", err)
	}
	if score != -1000 {
		t.Errorf("score after 3 failures = %f, want -1000", score)
	}

	client.rdb.Del(ctx, "proxy_pool")
	client.rdb.Del(ctx, "proxy_ranking")
}

func TestRetryCount(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, retryCountPrefix+"task-1")

	count, err := client.GetRetryCount(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetRetryCount() error = %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0 for new task", count)
	}

	err = client.IncrementRetryCount(ctx, "task-1")
	if err != nil {
		t.Fatalf("IncrementRetryCount() error = %v", err)
	}

	c2, err := client.GetRetryCount(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetRetryCount() after increment error = %v", err)
	}
	if c2 != 1 {
		t.Errorf("c2 = %d, want 1", c2)
	}

	err = client.IncrementRetryCount(ctx, "task-1")
	if err != nil {
		t.Fatalf("IncrementRetryCount() second error = %v", err)
	}

	got, err := client.GetRetryCount(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetRetryCount() after second increment error = %v", err)
	}
	if got != 2 {
		t.Errorf("got = %d, want 2", got)
	}
}

func TestSeenSetTTL(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, seenSetFetchKey)
	client.rdb.Del(ctx, seenSetParseKey)

	err := client.MarkFetchIDSeen(ctx, "fetch-1")
	if err != nil {
		t.Fatalf("MarkFetchIDSeen() error = %v", err)
	}

	ttl, err := client.rdb.TTL(ctx, seenSetFetchKey).Result()
	if err != nil {
		t.Fatalf("TTL() error = %v", err)
	}
	if ttl < seenSetFetchTTL-5*time.Second || ttl > seenSetFetchTTL+5*time.Second {
		t.Errorf("ttl = %v, want ~%v", ttl, seenSetFetchTTL)
	}

	err = client.MarkParseIDSeen(ctx, "parse-1")
	if err != nil {
		t.Fatalf("MarkParseIDSeen() error = %v", err)
	}

	ttl2, err := client.rdb.TTL(ctx, seenSetParseKey).Result()
	if err != nil {
		t.Fatalf("TTL() for parse key error = %v", err)
	}
	if ttl2 < seenSetParseTTL-5*time.Second || ttl2 > seenSetParseTTL+5*time.Second {
		t.Errorf("ttl2 = %v, want ~%v", ttl2, seenSetParseTTL)
	}
}

func TestFetcherRedisDeduplicationPath(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, seenSetFetchKey)
	client.rdb.Del(ctx, seenSetParseKey)

	client.MarkFetchIDSeen(ctx, "100")
	client.MarkFetchIDSeen(ctx, "200")

	seen, err := client.IsFetchIDSeen(ctx, "100")
	if err != nil {
		t.Fatalf("IsFetchIDSeen(100) error = %v", err)
	}
	if !seen {
		t.Error("IsFetchIDSeen(100) = false, want true after mark")
	}

	notSeen, err := client.IsFetchIDSeen(ctx, "999")
	if err != nil {
		t.Fatalf("IsFetchIDSeen(999) error = %v", err)
	}
	if notSeen {
		t.Error("IsFetchIDSeen(999) = true, want false for unmarked ID")
	}
}

func TestFetcherQueueAndMarkFlow(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, seenSetFetchKey)
	client.rdb.Del(ctx, seenSetParseKey)
	client.rdb.Del(ctx, fetchQueueKey)

	queueLen, _ := client.GetQueueLen(ctx)
	if queueLen != 0 {
		t.Errorf("initial queueLen = %d, want 0", queueLen)
	}

	task := models.FetchTask{MatchID: "555", URL: "https://api.example.com/555"}
	err := client.PushFetchTask(ctx, task)
	if err != nil {
		t.Fatalf("PushFetchTask() error = %v", err)
	}

	err = client.MarkFetchIDSeen(ctx, "555")
	if err != nil {
		t.Fatalf("MarkFetchIDSeen() error = %v", err)
	}

	seen, _ := client.IsFetchIDSeen(ctx, "555")
	if !seen {
		t.Error("IsFetchIDSeen(555) = false, want true after mark")
	}

	queueLen2, _ := client.GetQueueLen(ctx)
	if queueLen2 != 1 {
		t.Errorf("queueLen2 = %d, want 1", queueLen2)
	}
}

func TestFetcherParseFlow(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupRedisContainer(ctx, t)
	defer cleanup()

	client.rdb.Del(ctx, parseQueueKey)
	client.rdb.Del(ctx, seenSetParseKey)

	taskID := "parse-task-001"
	err := client.PushParseTask(ctx, taskID)
	if err != nil {
		t.Fatalf("PushParseTask() error = %v", err)
	}

	err = client.MarkParseIDSeen(ctx, taskID)
	if err != nil {
		t.Fatalf("MarkParseIDSeen() error = %v", err)
	}

	seen, _ := client.IsParseIDSeen(ctx, taskID)
	if !seen {
		t.Error("IsParseIDSeen(taskID) = false, want true after mark")
	}
}
