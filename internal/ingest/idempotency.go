package ingest

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Struct that protect from poccessing same `call_id`
// on level external system and ingest service
type Deduplicator struct {
	rdb *redis.Client
	ttl time.Duration
}

// Constructor for `Deduplicator`
func NewDeduplicator(rdb *redis.Client, ttl time.Duration) *Deduplicator {
	return &Deduplicator{rdb: rdb, ttl: ttl}
}

func (d *Deduplicator) SeenBefore(ctx context.Context, callID string) (bool, error) {
	key := "callmark:ingest:seen:" + callID
	wasSet, err := d.rdb.SetNX(ctx, key, 1, d.ttl).Result()
	if err != nil {
		return false, err
	}
	return !wasSet, nil
}
