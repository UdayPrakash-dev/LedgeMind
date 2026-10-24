// Package snapshot stores wallet snapshots in Redis. Redis is only a cache:
// every error here is safe to ignore, because the event store has the truth.
package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"ledgemind/wallet-service/domain"
)

// timeout keeps a slow or dead Redis from slowing down wallet requests.
const timeout = 100 * time.Millisecond

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	return &RedisStore{client: redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		MaxRetries:   -1, // fail fast; we fall back to the event store anyway
	})}
}

func key(walletID string) string { return "snap:wallet:" + walletID }

// Get returns (nil, nil) when there is no snapshot.
func (r *RedisStore) Get(ctx context.Context, walletID string) (*domain.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := r.client.Get(ctx, key(walletID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s domain.Snapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Put saves a snapshot, but never replaces a newer one with an older one.
func (r *RedisStore) Put(ctx context.Context, s domain.Snapshot) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	// Atomic "write only if newer" in one round trip.
	return putIfNewer.Run(ctx, r.client, []string{key(s.WalletID)}, s.Version, raw).Err()
}

var putIfNewer = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur then
  local ok, decoded = pcall(cjson.decode, cur)
  if ok and decoded.version and tonumber(decoded.version) >= tonumber(ARGV[1]) then
    return 0
  end
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`)

func (r *RedisStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return r.client.Ping(ctx).Err()
}

func (r *RedisStore) Close() error { return r.client.Close() }
