package estimator

import (
	"math"
	"strconv"
	"sync"
	"time"
)

const ftime float64 = 5.0 // 5 second avg

func MovingExpAvg(value, oldValue, fdtime, ftime float64) float64 {
	alpha := 1.0 - math.Exp(-fdtime/ftime)
	r := alpha * value + (1.0 - alpha) * oldValue
	return r
}

type RequestStat struct {
	UpdateTime	time.Time		`json:"-"`
	BPS		float64
	RPS		float64
}

func NewRequestStat() *RequestStat {
	return &RequestStat {
		UpdateTime:	time.Now(),
		BPS:		0,
		RPS:		0,
	}
}

type Estimator struct {
	sync.RWMutex

	RS		map[int]*RequestStat
}

func NewEstimator() *Estimator {
	return &Estimator {
		RS:	make(map[int]*RequestStat),
	}
}

func (e *Estimator) PushNolock(size uint64, status int) {
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

	fdelta := 1.0 // time passed since last update

	rs, ok := e.RS[status]
	if !ok {
		rs = NewRequestStat()
		e.RS[status] = rs
	} else {
		fdelta = time.Now().Sub(rs.UpdateTime).Seconds()
	}

	rs.RPS = MovingExpAvg(1.0, rs.RPS, fdelta, ftime)
	rs.BPS = MovingExpAvg(float64(size), rs.BPS, fdelta, ftime)

	rs.UpdateTime = time.Now()
}

func (e *Estimator) Push(size uint64, status int) {
	e.Lock()
	defer e.Unlock()

	e.PushNolock(size, status)
}


func (e *Estimator) Copy() (map[string]RequestStat) {
	e.Lock()
	defer e.Unlock()

	res := make(map[string]RequestStat)
	for k, v := range e.RS {
		fdelta := time.Now().Sub(v.UpdateTime).Seconds()
		if fdelta > ftime {
			v.RPS = MovingExpAvg(0, v.RPS, fdelta, ftime)
			v.BPS = MovingExpAvg(0, v.BPS, fdelta, ftime)
			v.UpdateTime = time.Now()
		}

		if v.RPS > 0.1 {
			res[strconv.Itoa(k)] = *v
		}
	}

	return res
}
