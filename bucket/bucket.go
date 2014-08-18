package bucket

import (
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/errors"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type BucketACL struct {
	version int32
	user    string
	token   string
	flags   uint64
}
type acl_json struct {
	User  string `json:"user"`
	Token string `json:"token"`
	Flags uint64 `json:"flags"`
}

type Bucket struct {
	Name	string
	Backend	map[string]Backend

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

func NewBucket(name string) Bucket {
	fmt.Printf("bucket: %s\n", name)
	return Bucket{
		Name:		name,
		Backend:	make(map[string]Backend),

		Rate:		1024 * 1024 * 1024 * 100,
		Packets:	0,
		Time:		time.Now(),
	}
}

type BucketCtl struct {
	Remote	[]string
	Bucket	[]Bucket
	Acl	map[string]BucketACL
}

var (
	BucketNamespace string = "bucket"
)

func (bctl *BucketCtl) open_acl(path string) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}

	data := make([]byte, 1024)

	count, err := file.Read(data)
	if err != nil {
		return
	}

	var jacl []acl_json
	err = json.Unmarshal(data[:count], &jacl)
	if err != nil {
		return
	}

	bctl.Acl = make(map[string]BucketACL)
	for _, a := range jacl {
		var e BucketACL

		e.user = a.User
		e.token = a.Token
		e.flags = a.Flags
		e.version = 1

		bctl.Acl[e.user] = e
	}

	return
}

func (bctl *BucketCtl) GetBucket() (bucket *Bucket) {
	sum := 0.0
	for i := range bctl.Bucket {
		b := &bctl.Bucket[i]
		sum += b.Rate
	}

	r := rand.Int63n(int64(sum))
	for i, _ := range bctl.Bucket {
		b := &bctl.Bucket[i]

		r -= int64(b.Rate)
		if r < 0 {
			return b
		}
	}

	// error, should never reach this point
	return &bctl.Bucket[rand.Intn(len(bctl.Bucket))]
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

func (bctl *BucketCtl) GetStat() (data []byte, err error) {
	remote := bctl.Remote[0]

	resp, err := http.Get(remote + "/stat/")
	if err != nil {
		log.Printf("Could not grab remote statistics from %s: %v", remote, err)
		return
	}
	defer resp.Body.Close()

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Could not read remote statistics from reply %s: %v", remote, err)
		return
	}

	return data, nil
}

func (bctl *BucketCtl) ParseStat(data []byte) (err error) {
	var jdata interface{}

	err = json.Unmarshal(data, &jdata)
	if err != nil {
		log.Printf("Could not parse statistics '%q': %v", data, err)
		return err
	}

	//log.Printf("%q\n", jdata)
	return nil
}

func (bctl *BucketCtl) CheckAuth(r *http.Request) (user string, err error) {
	if len(bctl.Acl) == 0 {
		err = nil
		return
	}

	user, recv_auth, err := auth.GetAuthInfo(r)
	if err != nil {
		return
	}

	acl, ok := bctl.Acl[user]
	if !ok {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: there is no user '%s' in ACL\n", r.URL, user))
		return
	}

	calc_auth, err := auth.GenerateSignature(acl.token, r.Method, r.URL, r.Header)
	if err != nil {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: hmac generation failed: %s\n", r.URL, err))
		return
	}

	if recv_auth != calc_auth {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: hmac mismatch: recv: '%s', calc: '%s'\n",
				r.URL, recv_auth, calc_auth))
		return
	}

	return
}

func (bctl *BucketCtl) GenAuthHeader(user string, r *http.Request) (sign string, err error) {
	sign = fmt.Sprintf("riftv1 %s:", user)
	if len(bctl.Acl) == 0 {
		err = nil
		return
	}

	acl, ok := bctl.Acl[user]
	if !ok {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: there is no user '%s' in ACL\n", r.URL, user))
		return
	}

	sign, err = auth.GenerateSignature(acl.token, r.Method, r.URL, r.Header)
	if err != nil {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: hmac generation failed: %s\n", r.URL, err))
		return
	}

	return
}

func NewBucketCtl(remote []string, bucket_path, acl_path string) (bctl BucketCtl, err error) {
	bctl = BucketCtl{
		Remote:	remote,
		Bucket: make([]Bucket, 0, 10),
		Acl:    make(map[string]BucketACL),
	}

	data, err := ioutil.ReadFile(bucket_path)
	if err != nil {
		return
	}

	for _, name := range strings.Split(string(data), "\n") {
		if len(name) > 0 {
			bctl.Bucket = append(bctl.Bucket, NewBucket(name))
		}
	}

	if len(bctl.Bucket) == 0 {
		log.Fatal("No buckets found in bucket file")
	}

	err = bctl.open_acl(acl_path)
	if err != nil {
		log.Fatalf("Failed to process ACL file: %q", err)
	}

	data, err = bctl.GetStat()
	if err != nil {
		log.Fatalf("Could not grab initial stats: %q", err)
	}

	err = bctl.ParseStat(data)
	if err != nil {
		log.Fatalf("Could not parse initial stats: %q", err)
	}

	return bctl, nil
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
	for _, x := range out[3].([]interface{}) {
		meta.groups = append(meta.groups, int32(x.(int64)))
	}
	meta.flags = uint64(out[4].(int64))
	meta.max_size = uint64(out[5].(int64))
	meta.max_key_num = uint64(out[6].(int64))

	return nil
}
