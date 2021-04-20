package cache

import (
	"github.com/gomodule/redigo/redis"
)

type RedisCache struct {
	pool *redis.Pool

	// hit cnt
	stat *Statistic
}

func NewRedisCache(name string, p *redis.Pool, m *MemCache) *RedisCache {
	c := &RedisCache{
		pool: p,
		stat: NewStatistic(name + "_reids"),
	}
	return c
}

// read item from redis
func (c *RedisCache) Get(key string, obj interface{}) (*Item, bool) {
	c.stat.addTotal(1)

	body, err := RedisGetString(key, c.pool)
	if err != nil && err != redis.ErrNil {
		return nil, false
	}
	if body == "" {
		return nil, false
	}

	var it Item
	it.Object = obj
	err = json.Unmarshal([]byte(body), &it)
	if err != nil {
		return nil, false
	}
	c.stat.addHit(1)
	return &it, true
}

func (c *RedisCache) Delete(keyPattern string) error {
	return RedisDelKey(keyPattern, c.pool)
}

func (c *RedisCache) Set(key, val string, ttl int) error {
	return RedisSetString(key, val, ttl, c.pool)
}
