// inspired from https://github.com/patrickmn/go-cache
package cache

import (
	"sync"
	"time"

	"github.com/seaguest/common/logger"
)

type MemCache struct {
	items sync.Map
	ci    time.Duration // 清理过期数据间隙时间
	stop  chan bool
	stat  *Statistic // hit cnt
}

// memcache will scan all objects per clean interval, and delete expired key.
func NewMemCache(name string, ci time.Duration) *MemCache {
	c := &MemCache{
		items: sync.Map{},
		ci:    ci,
		stop:  make(chan bool),
		stat:  NewStatistic(name + "_mem"),
	}

	c.StartScan()
	return c
}

func (c *MemCache) Set(k string, it *Item) {
	logger.Info("update mem: ", k, it)
	c.items.Store(k, it)
}

// start key scanning to delete expired keys
func (c *MemCache) StartScan() {
	go func() {
		ticker := time.NewTicker(c.ci)
		for {
			select {
			case <-ticker.C:
				c.DeleteExpired()
			case <-c.stop:
				ticker.Stop()
				return
			}
		}
	}()
}

// set clean interval
func (c *MemCache) SetCleanInterval(ci time.Duration) {
	c.ci = ci
}

// stop key scanning
func (c *MemCache) StopScan() {
	c.stop <- true
}

// return true if data is fresh
func (c *MemCache) Load(k string) (*Item, bool) {
	it, exists := c.Get(k)
	if !exists {
		return nil, false
	}
	return it, !it.Outdated()
}

// Get an item from the memcache. Returns the item or nil, and a bool indicating whether the key was found.
func (c *MemCache) Get(k string) (*Item, bool) {
	c.stat.addTotal(1) //记录一次缓存查询
	tmp, found := c.items.Load(k)
	logger.Info("get data from mem.", tmp, found)
	if !found {
		return nil, false
	}
	item := tmp.(*Item)
	if item.Expiration > 0 {
		if time.Now().UnixNano() > item.Expiration {
			return nil, false
		}
	}
	c.stat.addHit(1) //记录一次缓存命中
	return item, true
}

// Delete an item from the memcache. Does nothing if the key is not in the memcache.
func (c *MemCache) Delete(k string) {
	c.delete(k)
}

func (c *MemCache) delete(k string) (interface{}, bool) {
	c.items.Delete(k)
	return nil, false
}

// Delete all expired items from the memcache.
func (c *MemCache) DeleteExpired() {
	c.items.Range(func(key, value interface{}) bool {
		v := value.(*Item)
		k := key.(string)
		// delete outdate for memory cahce
		if v.Outdated() {
			c.delete(k)
		}
		return true
	})
}
