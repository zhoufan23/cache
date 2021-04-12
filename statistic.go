package cache

import (
	"sync/atomic"
	"time"

	"github.com/seaguest/common/logger"
)

type StatisticCnt struct {
	viewCnt int64 //周期性的可视化汇总
	cnt     int64 //计数
}

type Statistic struct {
	name  string //打点需要
	total StatisticCnt
	hit   StatisticCnt
}

func NewStatistic(n string) *Statistic {
	Statistic := &Statistic{
		name: n,
	}
	go Statistic.summary()
	return Statistic
}

func (s *Statistic) addTotal(n int) {
	atomic.AddInt64(&s.total.cnt, int64(n))
}

func (s *Statistic) addHit(n int) {
	atomic.AddInt64(&s.hit.cnt, int64(n))
}

func (s *Statistic) summary() {
	defer func() {
		if e := recover(); e != nil {
			logger.Error(e)
		}
	}()
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		s.total.viewCnt = atomic.SwapInt64(&s.total.cnt, 0)
		s.hit.viewCnt = atomic.SwapInt64(&s.hit.cnt, 0)
		// TODO gf 打点
	}
}
