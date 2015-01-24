package estimator

import (
	"math"
	"sync"
	"time"
)

type Estimator struct {
	sync.RWMutex

	BPS		float64
	RPS		map[int]float64
}

func NewEstimator() *Estimator {
	e := &Estimator {
		RPS: make(map[int]float64),
	}

	return e
}

func MovingExpAvg(value, oldValue, fdtime, ftime float64) float64 {
	alpha := 1.0 - math.Exp(-fdtime/ftime)
	r := alpha*value + (1.0-alpha)*oldValue
	return r
}

func (e *Estimator) Push(size uint64, duration time.Duration, status int) {
	e.Lock()
	defer e.Unlock()

	e.BPS = MovingExpAvg(float64(size), e.BPS, float64(duration), 1.0)

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

	e.RPS[status] = MovingExpAvg(1, e.RPS[status], float64(duration), 1.0)
}

func (e *Estimator) Read() *Estimator {
	e.RLock()
	defer e.RUnlock()

	res := NewEstimator()
	res.BPS = e.BPS
	for k, v := range e.RPS {
		res.RPS[k] = v
	}

	return res
}
