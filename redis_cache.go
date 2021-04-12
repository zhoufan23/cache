package cache

import (
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/seaguest/common/logger"
)

type RedisCache struct {
	pool *redis.Pool

	// RWMutex map for each cache key
	muxm sync.Map

	mem *MemCache

	// hit cnt
	stat *Statistic
}

func NewRedisCache(name string, p *redis.Pool, m *MemCache) *RedisCache {
	c := &RedisCache{
		pool: p,
		mem:  m,
		stat: NewStatistic(name + "_reids"),
	}
	return c
}

// store one mutex per key
// TODO, mux should be released when no more accessed
func (c *RedisCache) getMutex(key string) *sync.RWMutex {
	var mux *sync.RWMutex
	nMux := new(sync.RWMutex)
	if oMux, ok := c.muxm.LoadOrStore(key, nMux); ok {
		mux = oMux.(*sync.RWMutex)
		nMux = nil
	} else {
		mux = nMux
	}
	return mux
}

// read item from redis
func (c *RedisCache) Get(key string, obj interface{}) (*Item, bool) {
	mux := c.getMutex(key)
	mux.RLock()
	defer func() {
		mux.RUnlock()
	}()

	c.stat.addTotal(1)
	// check if item is fresh in mem, return directly if true
	// 防缓存穿透
	if v, fresh := c.mem.Load(key); fresh {
		return v, true
	}

	body, err := RedisGetString(key, c.pool)
	logger.Info("get data from redis.", body, err)
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
	c.mem.Set(key, &it)
	return &it, true
}

// load redis with loader function
// when sync is true, obj must be set before return.
func (c *RedisCache) load(key string, obj interface{}, ttl int, f LoadFunc, sync bool) error {
	mux := c.getMutex(key)
	mux.Lock()
	defer func() {
		mux.Unlock()
	}()

	// 防缓存穿透
	if it, fresh := c.mem.Load(key); fresh {
		if sync {
			return clone(it.Object, obj)
		}
		return nil
	}

	o, err := f()
	logger.Info("run f.", o, err)
	if err != nil {
		return err
	}

	if sync {
		if err := clone(o, obj); err != nil {
			return err
		}
	}

	// update memcache
	it := NewItem(o, ttl)

	rdsTTL := (it.Expiration - time.Now().UnixNano()) / int64(time.Second)
	bs, _ := json.Marshal(it)
	err = RedisSetString(key, string(bs), int(rdsTTL), c.pool)
	if err != nil {
		return err
	}

	c.mem.Set(key, it)
	return nil
}

func (c *RedisCache) Delete(keyPattern string) error {
	return RedisDelKey(keyPattern, c.pool)
}
