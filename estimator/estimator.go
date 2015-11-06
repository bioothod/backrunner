package estimator

import (
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"time"
)

const EstimatorRange int = 3

func MovingExpAvg(value, oldValue, fdtime, ftime float64) float64 {
	alpha := 1.0 - math.Exp(-fdtime/ftime)
	r := alpha * value + (1.0 - alpha) * oldValue
	return r
}

type Second struct {
	Second		int
	BPS		uint64
	RPS		uint64
}

type RequestStat struct {
	SStat		[]Second		`json:"-"`
	BPS		float64
	RPS		float64
}

func NewRequestStat() *RequestStat {
	return &RequestStat {
		BPS:		0,
		RPS:		0,
		SStat:		make([]Second, EstimatorRange),
	}
}

func (r *RequestStat) Copy(start int) {
	for i := 0; i < EstimatorRange; i++ {
		idx := start - i - 1
		if idx < 0 {
			idx = EstimatorRange - i - 1
		}

		s := r.SStat[idx]
		r.RPS = MovingExpAvg(float64(s.RPS), r.RPS, float64(EstimatorRange - i), float64(EstimatorRange))
		r.BPS = MovingExpAvg(float64(s.BPS), r.BPS, float64(EstimatorRange - i), float64(EstimatorRange))
	}
}

type Estimator struct {
	sync.Mutex

	RS		map[int]*RequestStat
}

func NewEstimator() *Estimator {
	return &Estimator {
		RS:	make(map[int]*RequestStat),
	}
}

func (e *Estimator) PushNolock(requests, size uint64, status int) {
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

	rs, ok := e.RS[status]
	if !ok {
		rs = NewRequestStat()
		e.RS[status] = rs
	}

	second := time.Now().Second()

	idx := second % EstimatorRange
	ss := &rs.SStat[idx]

	if ss.Second != second {
		ss.Second = second
		ss.RPS = 0
		ss.BPS = 0
	}

	ss.RPS += requests
	ss.BPS += size
}

func (e *Estimator) Push(size uint64, status int) {
	e.Lock()
	defer e.Unlock()

	e.PushNolock(1, size, status)
}

func (e *Estimator) MarshalJSON() ([]byte, error) {
	second := time.Now().Second()
	idx := second % EstimatorRange

	res := make(map[string]RequestStat)

	e.Lock()
	for k, v := range e.RS {
		e.PushNolock(0, 0, k)

		v.Copy(idx)

		if v.RPS > 0.1 {
			res[strconv.Itoa(k)] = *v
		}
	}
	e.Unlock()

	return json.Marshal(res)
}
