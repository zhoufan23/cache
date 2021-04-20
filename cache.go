package cache

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
	"github.com/mohae/deepcopy"
)

const (
	lazyFactor    = 256
	delKeyChannel = "delkey"
	cleanInterval = time.Second * 10
)

type Cache struct {
	// cache name
	name string

	// RWMutex map for each cache key
	muxm sync.Map

	// redis connection
	pool *redis.Pool

	// rds cache, handles redis level cache
	rds *RedisCache

	// mem cache, handles in-memory cache
	mem *MemCache

	// in debug mode, ttl is set to 1s, mem clean interval is set to 1s
	debug bool
}

type LoadFunc func() (interface{}, error)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(cacheName, redisAddr, redisPassword string, maxConnection int) *Cache {
	c := &Cache{
		name: cacheName,
	}
	c.pool = GetRedisPool(redisAddr, redisPassword, maxConnection)
	c.mem = NewMemCache(c.name, cleanInterval)
	c.rds = NewRedisCache(c.name, c.pool, c.mem)

	// subscribe key deletion
	go c.subscribe(delKeyChannel)
	return c
}

// enable debug mode
func (c *Cache) EnableDebug() {
	c.debug = true
	c.mem.StopScan()
	c.mem.SetCleanInterval(time.Second)
	c.mem.StartScan()
}

// sync memcache from redis
func (c *Cache) syncMem(key string, copy interface{}, ttl int, f LoadFunc) {
	it, ok := c.rds.Get(key, copy)

	// if key not exists in redis or data outdated, then load from redis
	if !ok || it.Outdated() {
		c.load(key, nil, ttl, f, false)
		return
	}
	c.mem.Set(key, it)
}

// store one mutex per key
// TODO, mux should be released when no more accessed
func (c *Cache) getMutex(key string) *sync.RWMutex {
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

func (c *Cache) load(key string, obj interface{}, ttl int, f LoadFunc, sync bool) error {
	o, err := f()
	if err != nil {
		return err
	}

	if sync {
		if err := clone(o, obj); err != nil {
			return err
		}
	}

	// update redis cache
	it := NewItem(o, ttl)
	rdsTTL := (it.Expiration - time.Now().UnixNano()) / int64(time.Second)
	bs, _ := json.Marshal(it)

	if err := c.rds.Set(key, string(bs), int(rdsTTL)); err != nil {
		return err
	}

	// update mem cache
	c.mem.Set(key, it)
	return nil
}

func (c *Cache) getObjectWithExpiration(key string, obj interface{}, ttl int, f LoadFunc) error {
	// 查询本地缓存
	v, ok := c.mem.Get(key)
	if ok { //本地缓存查询成功
		if v.Outdated() { //数据实效性不满足时需异步更新
			to := deepcopy.Copy(obj)
			go c.syncMem(key, to, ttl, f)
		}
		return clone(v.Object, obj)
	}

	// 不存在 or 数据过期
	// 防缓存穿透
	mux := c.getMutex(key)
	mux.RLock()
	defer func() {
		mux.RUnlock()
	}()

	// 获取互斥锁成功后判断本地缓存实效性是否ok
	if v, fresh := c.mem.Load(key); fresh {
		return clone(v.Object, obj)
	}

	// 查询redis缓存
	v, ok = c.rds.Get(key, obj)
	if ok { //redis缓存查询成功
		if v.Outdated() { //数据实效性不满足时需异步更新
			go c.load(key, nil, ttl, f, false)
		} else {
			c.mem.Set(key, v) // 更新本地缓存
		}
		return clone(v.Object, obj)
	}
	return c.load(key, obj, ttl, f, true)
}

func (c *Cache) GetObject(key string, obj interface{}, ttl int, f LoadFunc) error {
	// is debug is enabled, set all ttl to 1s, clean interval to 1s
	if c.debug {
		ttl = 1
	}
	return c.getObjectWithExpiration(key, obj, ttl, f)
}

// notify all cache nodes to delete key
func (c *Cache) Delete(key string) error {
	return RedisPublish(delKeyChannel, key, c.pool)
}

// redis subscriber for key deletion
func (c *Cache) subscribe(key string) error {
	conn := c.pool.Get()
	defer conn.Close()

	psc := redis.PubSubConn{Conn: conn}
	if err := psc.Subscribe(key); err != nil {
		return err
	}

	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			key := string(v.Data)
			c.delete(key)
		case error:
			return v
		}
	}
}

func (c *Cache) delete(keyPattern string) error {
	c.mem.Delete(keyPattern)
	return c.rds.Delete(keyPattern)
}

// clone object to return, to avoid dirty data
func clone(src, dst interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint(r))
			return
		}
	}()

	v := deepcopy.Copy(src)
	if reflect.ValueOf(v).IsValid() {
		reflect.ValueOf(dst).Elem().Set(reflect.Indirect(reflect.ValueOf(v)))
	}
	return
}
