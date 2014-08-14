package bucket

import (
	"encoding/hex"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/elliptics-go/elliptics"
	"github.com/vmihailenco/msgpack"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"syscall"
	"sync/atomic"
	"time"
)

const BucketNamespace string = "bucket"

type BucketACL struct {
	version int32
	user    string
	token   string
	flags   uint64
}

type ExtractError struct {
	reason string
	out    []interface{}
}

func (err *ExtractError) Error() string {
	return fmt.Sprintf("%s: %v", err.reason, err.out)
}

type BucketMsgpack struct {
	version     int32
	bucket      string
	acl         map[string]BucketACL
	groups      []int32
	flags       uint64
	max_size    uint64
	max_key_num uint64
	reserved    [3]uint64
}

func (meta *BucketMsgpack) ExtractMsgpack(out []interface{}) (err error) {
	if len(out) < 8 {
		return &ExtractError{
			reason: fmt.Sprintf("array length: %d, must be at least 8", len(out)),
			out:    out,
		}
	}
	meta.version = int32(out[0].(int64))
	if meta.version != 1 {
		return &ExtractError{
			reason: fmt.Sprintf("unsupported metadata version %d", meta.version),
			out:    out,
		}
	}
	meta.bucket = out[1].(string)

	meta.acl = make(map[string]BucketACL)
	for _, i := range out[2].(map[interface{}]interface{}) {
		var acl BucketACL
		x := i.([]interface{})

		if v, ok := x[0].(int32); ok {
			acl.version = v
		}
		if v, ok := x[1].(string); ok {
			acl.user = v
		}
		if v, ok := x[2].(string); ok {
			acl.token = v
		}
		if v, ok := x[3].(uint64); ok {
			acl.flags = v
		}

		if len(acl.user) != 0 {
			meta.acl[acl.user] = acl
		}
	}

	for _, x := range out[3].([]interface{}) {
		meta.groups = append(meta.groups, int32(x.(int64)))
	}
	meta.flags = uint64(out[4].(int64))
	meta.max_size = uint64(out[5].(int64))
	meta.max_key_num = uint64(out[6].(int64))

	return nil
}

type Bucket struct {
	Name	string
	Backend	map[string]Backend

	meta	BucketMsgpack

	Rate	float64
	Packets int64
	Time    time.Time
}

type Backend struct {
	Id	string // hostname in elliptics 2.25
	Rate	float64
	Packets int64
	Time    time.Time
}

func NewBackend(name string) Backend {
	return Backend {
		Id:		name,
		Rate:		1024 * 1024 * 1024 * 100,
		Time:		time.Now(),
		Packets:	0,
	}
}

type BucketCtl struct {
	e	*etransport.Elliptics
	Bucket	[]*Bucket
}

func (bctl *BucketCtl) GetBucket() (bucket *Bucket) {
	sum := 0.0
	for i := range bctl.Bucket {
		b := bctl.Bucket[i]
		sum += b.Rate
	}

	r := rand.Int63n(int64(sum))
	for i, _ := range bctl.Bucket {
		b := bctl.Bucket[i]

		r -= int64(b.Rate)
		if r < 0 {
			return b
		}
	}

	// error, should never reach this point
	return bctl.Bucket[rand.Intn(len(bctl.Bucket))]
}

func MovingExpAvg(value, oldValue, fdtime, ftime float64) float64 {
	alpha := 1.0 - math.Exp(-fdtime/ftime)
	r := alpha*value + (1.0-alpha)*oldValue
	return r
}

func (bucket *Bucket) SetRate(rate float64) {
	t := time.Now()
	diff := t.Sub(bucket.Time).Seconds()
	bucket.Rate = MovingExpAvg(rate, bucket.Rate, float64(diff), 1.0)

	bucket.Time = t
	atomic.AddInt64(&bucket.Packets, 1)
}

func (bucket *Bucket) HalfRate() {
	t := time.Now()
	diff := t.Sub(bucket.Time).Seconds()
	bucket.Rate = MovingExpAvg(bucket.Rate/2.0, bucket.Rate, float64(diff), 1.0)

	bucket.Time = t
}

func (b *Bucket) check_auth(r *http.Request) (err error) {
	if len(b.meta.acl) == 0 {
		err = nil
		return
	}

	user, recv_auth, err := auth.GetAuthInfo(r)
	if err != nil {
		return
	}

	acl, ok := b.meta.acl[user]
	if !ok {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("auth: there is no user '%s' in ACL", user))
		return
	}

	calc_auth, err := auth.GenerateSignature(acl.token, r.Method, r.URL, r.Header)
	if err != nil {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("auth: hmac generation failed: %s", err))
		return
	}

	if recv_auth != calc_auth {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("auth: hmac mismatch: recv: '%s', calc: '%s'",
				recv_auth, calc_auth))
		return
	}

	return
}

func (bctl *BucketCtl) Upload(key string, req *http.Request) (reply map[string]interface{}, bucket *Bucket, err error) {
	bucket = bctl.GetBucket()

	err = bucket.check_auth(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("upload: %s", errors.ErrorData(err)))
		return
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("upload: could not read data: %v", err))
		return
	}
	defer req.Body.Close()

	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("upload: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.meta.groups)


	var info []interface{}
	var egroups, sgroups []uint32

	egroups = make([]uint32, 0)
	sgroups = make([]uint32, 0)


	for l := range s.WriteData(key, data) {
		ret := make(map[string]interface{})
		if l.Error() != nil {
			egroups = append(egroups, l.Cmd().ID.Group)

			ret["error"] = fmt.Sprintf("%v", l.Error())
		} else {
			sgroups = append(sgroups, l.Cmd().ID.Group)

			ret["id"] = hex.EncodeToString(l.Cmd().ID.ID)
			ret["csum"] = hex.EncodeToString(l.Info().Csum)
			ret["filename"] = l.Path()
			ret["size"] = l.Info().Size
			ret["offset-within-data-file"] = l.Info().Offset
			ret["mtime"] = l.Info().Mtime.String()
			ret["server"] = l.StorageAddr().String()
		}

		info = append(info, ret)
	}

	reply = make(map[string]interface{})
	reply["info"] = info
	reply["success-groups"] = sgroups
	reply["error-groups"] = egroups

	return
}

func (bctl *BucketCtl) Get(bname, key string, req *http.Request) (resp []byte, err error) {
	var bucket *Bucket = nil
	ok := false

	for _, bucket = range bctl.Bucket {
		if bucket.Name == bname {
			ok = true
			break
		}
	}

	if !ok {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("get: could not find bucket '%s'", bname))
		return
	}

	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("get: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.meta.groups)

	for rd := range s.ReadData(key) {
		if rd.Error() != nil {
			err = rd.Error()

			code := elliptics.ErrorStatus(err)
			message := elliptics.ErrorData(err)
			status := http.StatusBadRequest

			switch syscall.Errno(-code) {
			case syscall.ENXIO:
				status = http.StatusServiceUnavailable
			case syscall.ENOENT:
				status = http.StatusNotFound
			}

			err = errors.NewKeyError(req.URL.String(), status,
				fmt.Sprintf("get: could not read data: elliptics-code: %d, elliptics-message: %s",
					code, message))
			return
		}

		resp = rd.Data()
	}
	return
}

func (bctl *BucketCtl) NewBucket(name string) (bucket *Bucket, err error) {
	ms, err := bctl.e.MetadataSession()
	if err != nil {
		log.Printf("%s: could not create metadata session: %v", name, err)
		return
	}

	ms.SetNamespace(BucketNamespace)

	b := &Bucket {
		Name:		name,
		Backend:	make(map[string]Backend),

		Rate:		1024 * 1024 * 1024 * 100,
		Packets:	0,
		Time:		time.Now(),
	}

	for rd := range ms.ReadData(name) {
		if rd.Error() != nil {
			err = rd.Error()

			log.Printf("%s: could not read bucket metadata: %v", name, err)
			return
		}

		var out []interface{}
		err = msgpack.Unmarshal([]byte(rd.Data()), &out)
		if err != nil {
			log.Printf("%s: could not parse bucket metadata: %v", name, err)
			return
		}

		err = b.meta.ExtractMsgpack(out)
		if err != nil {
			log.Printf("%s: unsupported msgpack data:", name, err)
		}

		log.Printf("%s: groups: %v, acl: %v\n", b.Name, b.meta.groups, b.meta.acl)
		bucket = b
		return
	}

	err = errors.NewKeyError(name, http.StatusNotFound, "could not read bucket data: ReadData() returned nothing")
	return
}

func NewBucketCtl(bucket_path string, e *etransport.Elliptics) (bctl *BucketCtl, err error) {
	bctl = &BucketCtl {
		e:		e,
		Bucket:		make([]*Bucket, 0, 10),
	}

	data, err := ioutil.ReadFile(bucket_path)
	if err != nil {
		return
	}

	for _, name := range strings.Split(string(data), "\n") {
		if len(name) > 0 {
			b, err := bctl.NewBucket(name)
			if err != nil {
				continue
			}

			bctl.Bucket = append(bctl.Bucket, b)
		}
	}

	if len(bctl.Bucket) == 0 {
		log.Fatal("No buckets found in bucket file")
	}

	return bctl, nil
}
