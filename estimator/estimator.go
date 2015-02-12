package estimator

import (
	"sync"
	"time"
)

const EstimatorInterval time.Duration = 5 * time.Second

type Stat struct {
	UpdateTime	time.Time
	BPS		uint64
	RPS		map[int]uint64
}

func (src *Stat) CopyInto(dst *Stat) {
	dst.UpdateTime = src.UpdateTime

	dst.BPS = src.BPS
	for k, v := range src.RPS {
		dst.RPS[k] = v
	}
}

func (s *Stat) Clear() {
	s.BPS = 0
	s.UpdateTime = time.Now().Add(EstimatorInterval)
	s.RPS = make(map[int]uint64)
}

func (s *Stat) Adjust(tm time.Time) {
	d := tm.Sub(s.UpdateTime).Seconds()
	s.BPS = uint64(float64(s.BPS) / d)
	for k, v := range s.RPS {
		s.RPS[k] = uint64(float64(v) / d)
	}
}


type Estimator struct {
	sync.RWMutex


	Cache		Stat
	Current		Stat
}

func NewEstimator() *Estimator {
	e := &Estimator {
	}

	e.Cache.Clear()
	e.Current.Clear()

	return e
}

func (e *Estimator) UpdateCache() {
	tm := time.Now()

	if tm.After(e.Current.UpdateTime) {
		e.Current.CopyInto(&e.Cache)
		e.Cache.Adjust(tm)

		e.Current.Clear()
	}
}

func (e *Estimator) Push(size uint64, status int) {
	e.Lock()
	defer e.Unlock()

	switch {
	case status >= 200 && status < 300:
		status = 200
	case status >= 300 && status < 400:
		status = 300
	case status >= 400 && status < 500:
		status = 400
	case status >= 500 && status < 600:
		status = 500
	}

	e.UpdateCache()

	e.Current.RPS[status] += 1
	if (uint64(size) > 0) {
		e.Current.BPS += size
	}
}

func (e *Estimator) Read() *Stat {
	e.RLock()
	defer e.RUnlock()

	e.UpdateCache()

	res := &Stat {}
	res.Clear()

	e.Cache.CopyInto(res)
	return res
}
