package bucket

import (
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"github.com/bioothod/elliptics-go/elliptics"
	"fmt"
	"io/ioutil"
	"log"
	//"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// time to write 1 byte into error bucket in seconds
	// this is randomly selected error gain for buckets where upload has failed
	BucketWriteErrorPain float64	= 10000000000.0

	PainNoStats float64		= 15000000000.0
	PainStatError float64		= 15000000000.0
	PainNoGroup float64		= 15000000000.0
	PainNoFreeSpaceSoft float64	= 5000000000.0
	PainNoFreeSpaceHard float64	= 50000000000.0
)

func URIOffsetSize(req *http.Request) (offset uint64, size uint64, err error) {
	offset = 0
	size = 0

	q := req.URL.Query()
	offset_str := q.Get("offset")
	if offset_str != "" {
		offset, err = strconv.ParseUint(offset_str, 0, 64)
		if err != nil {
			err = fmt.Errorf("could not parse offset URI: %s: %v", offset_str, err)
			return
		}
	}

	size_str := q.Get("size")
	if size_str != "" {
		size, err = strconv.ParseUint(size_str, 0, 64)
		if err != nil {
			err = fmt.Errorf("could not parse size URI: %s: %v", size_str, err)
			return
		}
	}

	return offset, size, nil
}

type BucketCtl struct {
	sync.RWMutex

	bucket_path		string
	e			*etransport.Elliptics

	proxy_config_path	string
	Conf			*config.ProxyConfig

	signals			chan os.Signal

	BucketTimer		*time.Timer
	BucketStatTimer		*time.Timer

	StatTime		time.Time

	// time when previous defragmentation scan was performed
	DefragTime		time.Time

	// buckets used for automatic write bucket selection,
	// i.e. when client doesn't provide bucket name and we select it
	// according to its performance and capacity
	Bucket			[]*Bucket

	// buckets used by clients directly, i.e. when client explicitly says
	// he wants to work with bucket named 'X'
	BackBucket		[]*Bucket
}

func (bctl *BucketCtl) AllBuckets() []*Bucket {
	out := bctl.Bucket
	return append(out, bctl.BackBucket...)
}

func (bctl *BucketCtl) FindBucketRO(name string) *Bucket {
	bctl.RLock()
	defer bctl.RUnlock()

	for _, b := range bctl.AllBuckets() {
		if b.Name == name {
			return b
		}
	}

	return nil
}

func (bctl *BucketCtl) FindBucket(name string) (bucket *Bucket, err error) {
	bucket = bctl.FindBucketRO(name)
	if bucket == nil {
		b, err := ReadBucket(bctl.e, name)
		if err != nil {
			return nil, fmt.Errorf("%s: could not find and read bucket: %v", name, err.Error())
		}

		bctl.Lock()
		defer bctl.Unlock()

		bctl.BackBucket = append(bctl.BackBucket, b)
		bucket = b
	}

	return bucket, nil
}

func (bctl *BucketCtl) BucketStatUpdate() (err error) {
	stat, err := bctl.e.Stat()
	if err != nil {
		return err
	}

	bctl.Lock()
	defer bctl.Unlock()

	bctl.StatTime = stat.Time

	buckets := 0
	for _, b := range bctl.AllBuckets() {
		buckets++
		b.Group = make(map[uint32]*elliptics.StatGroup)

		for _, group := range b.Meta.Groups {
			sg, ok := stat.Group[group]
			if ok {
				b.Group[group] = sg
			}
		}
	}

	// run defragmentation scan
	bctl.ScanBuckets()

	return
}

func (bctl *BucketCtl) GetBucket(key string, req *http.Request) (bucket *Bucket) {
	s, err := bctl.e.MetadataSession()
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("get-bucket: could not create metadata session: %v", err))
		return bctl.Bucket[rand.Intn(len(bctl.Bucket))]
	}

	bctl.RLock()
	defer bctl.RUnlock()

	type bucket_stat struct {
		Bucket		*Bucket
		SuccessGroups	[]uint32
		ErrorGroups	[]uint32
		Pain		float64
		Range		float64
	}

	stat := make([]*bucket_stat, 0)

	bctl.RLock()

	for _, b := range bctl.Bucket {
		bs := &bucket_stat {
			Bucket:		b,
			SuccessGroups:	make([]uint32, 0),
			ErrorGroups:	make([]uint32, 0),
			Pain:		0.0,
			Range:		0.0,
		}

		for group_id, sg := range b.Group {
			st, err := sg.FindStatBackend(s, key, group_id)
			if err != nil {
				// there is no statistics for given address+backend, which should host our data
				// do not allow to write into the bucket which contains given address+backend

				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoStats
				continue
			}

			if st.Error.Code != 0 {
				// this is usually a timeout error

				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainStatError
				continue
			}

			free_space_rate := 1.0 - float64(st.VFS.BackendUsedSize + uint64(req.ContentLength)) / float64(st.VFS.TotalSizeLimit)
			if free_space_rate <= bctl.Conf.Proxy.FreeSpaceRatioHard {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoFreeSpaceHard
			} else if free_space_rate <= bctl.Conf.Proxy.FreeSpaceRatioSoft {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoFreeSpaceSoft
			} else {
				bs.SuccessGroups = append(bs.SuccessGroups, group_id)

				free_space_pain := 1.0 / (free_space_rate - bctl.Conf.Proxy.FreeSpaceRatioSoft)
				if free_space_pain >= PainNoFreeSpaceSoft {
					free_space_pain = PainNoFreeSpaceSoft * 0.8
				}

				bs.Pain += free_space_pain
			}

			bs.Pain += st.PID.Pain

			log.Printf("find-bucket: url: %s, bucket: %s, group: %d: content-length: %d, free-space-rate: %f, backend-pain: %f, pain: %f\n",
				req.URL.String(), b.Name, group_id, req.ContentLength, free_space_rate, st.PID.Pain, bs.Pain)
		}

		total_groups := len(bs.SuccessGroups) + len(bs.ErrorGroups)
		diff := 0
		if len(b.Meta.Groups) > total_groups {
			diff += len(b.Meta.Groups) - total_groups
		}

		bs.Pain += float64(diff) * PainNoGroup

		log.Printf("find-bucket: url: %s, bucket: %s, content-length: %d, groups: %v, success-groups: %v, error-groups: %v, pain: %f\n",
			req.URL.String(), b.Name, req.ContentLength, b.Meta.Groups, bs.SuccessGroups, bs.ErrorGroups, bs.Pain)

		// do not even consider buckets without free space even in one group
		if bs.Pain >= PainNoFreeSpaceHard {
			continue
		}

		if bs.Pain != 0 {
			bs.Range = 1.0 / bs.Pain
			if bs.Range == 0 {
				bs.Range = 1
			}
		} else {
			bs.Range = 1.0
		}

		stat = append(stat, bs)
	}

	bctl.RUnlock()

	// there are no buckets suitable for this request
	// either there is no space in either bucket, or there are no buckets at all
	if len(stat) == 0 {
		return nil
	}

	for {
		need_multiple := false
		multiple := 10.0

		for _, bs := range stat {
			if bs.Range < multiple {
				need_multiple = true

				tmp := multiple / bs.Range
				if tmp > multiple {
					multiple = tmp
				}

				break
			}
		}

		if !need_multiple {
			break
		} else {
			for _, bs := range stat {
				bs.Range *= multiple
			}
		}
	}

	var sum float64 = 0.0
	for _, bs := range stat {
		sum += bs.Range
	}

	r := rand.Int63n(int64(sum))
	for _, bs := range stat {
		r -= int64(bs.Range)
		if r <= 0 {
			log.Printf("find-bucket: selected bucket: %s, groups: %v, pain: %f\n",
				bs.Bucket.Name, bs.Bucket.Meta.Groups, bs.Pain)
			return bs.Bucket
		}
	}

	return nil
}

func (bctl *BucketCtl) bucket_upload(bucket *Bucket, key string, req *http.Request) (reply *reply.LookupResult, err error) {
	err = bucket.check_auth(req, BucketAuthWrite)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("upload: %s", errors.ErrorData(err)))
		return
	}

	lheader, ok := req.Header["Content-Length"]
	if !ok {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			"upload: there is no Content-Length header")
		return
	}

	total_size, err := strconv.ParseUint(lheader[0], 0, 64)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("upload: invalid content length conversion: %v", err))
		return
	}

	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("upload: could not create data session: %v", err))
		return
	}

	s.SetFilter(elliptics.SessionFilterAll)
	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)
	s.SetTimeout(100)

	offset, _, err := URIOffsetSize(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("upload: %v", err))
		return
	}

	start := time.Now()

	reply, err = bucket.lookup_serialize(true, s.WriteData(key, req.Body, offset, total_size))


	// PID controller should aim at some destination performance point
	// it can be velocity pf the vehicle or deisred write rate
	//
	// Let's consider our desired control point as number of useconds  needed to write 1 byte into the storage
	// In the ideal world it will be 0.01 or 100 MB/s write speed

	time_us := time.Since(start).Nanoseconds() / 1000
	e := float64(time_us) / float64(total_size) - 0.01

	bctl.Lock()

	for _, group_id := range reply.SuccessGroups {
		sg, ok := bucket.Group[group_id]
		if ok {
			st, back_err := sg.FindStatBackend(s, key, group_id)
			if back_err == nil {
				old_pain := st.PID.Pain
				st.PIDUpdate(e)

				log.Printf("bucket-upload: bucket: %s, key: %s, size: %d, time: %d us, success group: %d, e: %f, pain: %f -> %f\n",
					bucket.Name, key, total_size, time_us, group_id, e, old_pain, st.PID.Pain)
			}
		}
	}

	error_groups := reply.ErrorGroups
	if len(reply.SuccessGroups) == 0 {
		error_groups = bucket.Meta.Groups
	}

	for _, group_id := range error_groups {
		sg, ok := bucket.Group[group_id]
		if ok {
			st, back_err := sg.FindStatBackend(s, key, group_id)
			if back_err == nil {
				old_pain := st.PID.Pain
				st.PIDUpdate(BucketWriteErrorPain)

				log.Printf("bucket-upload: bucket: %s, key: %s, size: %d, time: %d us, error group: %d, e: %f, pain: %f -> %f\n",
					bucket.Name, key, total_size, time_us, group_id, e, old_pain, st.PID.Pain)
			}
		}
	}

	bctl.Unlock()

	return
}

func (bctl *BucketCtl) Upload(key string, req *http.Request) (reply *reply.LookupResult, bucket *Bucket, err error) {
	bucket = bctl.GetBucket(key, req)
	if bucket == nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("there are no buckets with free space available"))
		return
	}

	reply, err = bctl.bucket_upload(bucket, key, req)
	return
}

func (bctl *BucketCtl) BucketUpload(bucket_name, key string, req *http.Request) (reply *reply.LookupResult, bucket *Bucket, err error) {
	bucket, err = bctl.FindBucket(bucket_name)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, err.Error())
		return
	}

	reply, err = bctl.bucket_upload(bucket, key, req)
	return
}

func (bctl *BucketCtl) Get(bname, key string, req *http.Request) (resp []byte, err error) {
	bucket, err := bctl.FindBucket(bname)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, err.Error())
		return
	}

	err = bucket.check_auth(req, BucketAuthEmpty)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("get: %s", errors.ErrorData(err)))
		return
	}

	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("get: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)

	offset, size, err := URIOffsetSize(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("get: %v", err))
		return
	}

	for rd := range s.ReadData(key, offset, size) {
		err = rd.Error()
		if err != nil {
			err = errors.NewKeyErrorFromEllipticsError(rd.Error(), req.URL.String(),
				"get: could not read data")
			continue
		}

		resp = rd.Data()
		return
	}
	return
}

func (bctl *BucketCtl) Stream(bname, key string, w http.ResponseWriter, req *http.Request) (err error) {
	bucket, err := bctl.FindBucket(bname)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, err.Error())
		return
	}

	err = bucket.check_auth(req, BucketAuthEmpty)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("stream: %s", errors.ErrorData(err)))
		return
	}


	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("stream: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)

	offset, size, err := URIOffsetSize(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("stream: %v", err))
		return
	}

	err = s.StreamHTTP(key, offset, size, w)
	if err != nil {
		err = errors.NewKeyErrorFromEllipticsError(err, req.URL.String(), "stream: could not stream data")
		return
	}

	return
}


func (bctl *BucketCtl) Lookup(bname, key string, req *http.Request) (reply *reply.LookupResult, err error) {
	bucket, err := bctl.FindBucket(bname)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, err.Error())
		return
	}

	err = bucket.check_auth(req, BucketAuthEmpty)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("upload: %s", errors.ErrorData(err)))
		return
	}


	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("lookup: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)

	reply, err = bucket.lookup_serialize(false, s.ParallelLookup(key))
	return
}

func (bctl *BucketCtl) Delete(bname, key string, req *http.Request) (err error) {
	bucket, err := bctl.FindBucket(bname)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, err.Error())
		return
	}

	err = bucket.check_auth(req, BucketAuthWrite)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("upload: %s", errors.ErrorData(err)))
		return
	}


	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("delete: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)

	for r := range s.Remove(key) {
		err = r.Error()
	}

	return
}

func (bctl *BucketCtl) BulkDelete(bname string, keys []string, req *http.Request) (reply map[string]interface{}, err error) {
	reply = make(map[string]interface{})

	bucket, err := bctl.FindBucket(bname)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, err.Error())
		return
	}

	err = bucket.check_auth(req, BucketAuthWrite)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), errors.ErrorStatus(err),
			fmt.Sprintf("upload: %s", errors.ErrorData(err)))
		return
	}


	s, err := bctl.e.DataSession(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("bulk_delete: could not create data session: %v", err))
		return
	}

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)

	for r := range s.BulkRemove(keys) {
		err = r.Error()
		if err != nil {
			reply[r.Key()] = err.Error()
		}
	}

	err = nil

	return
}

type BucketStat struct {
	Group		map[string]*elliptics.StatGroupData
	Meta		*BucketMsgpack
	NeedRecovery	string
}

type BctlStat struct {
	Buckets		map[string]*BucketStat
	StatTime	string
}

func (bctl *BucketCtl) Stat(req *http.Request) (reply *BctlStat, err error) {
	bctl.RLock()
	defer bctl.RUnlock()

	reply = &BctlStat {
		Buckets:		make(map[string]*BucketStat),
		StatTime:		bctl.StatTime.String(),
	}

	for _, b := range bctl.AllBuckets() {
		bs := &BucketStat {
			Group:	make(map[string]*elliptics.StatGroupData),
			Meta:	&b.Meta,
			NeedRecovery:	"not-needed",
		}

		for group, sg := range b.Group {
			sg_data := sg.StatGroupData()
			bs.Group[fmt.Sprintf("%d", group)] = sg_data

			for _, tmp := range bs.Group {
				if tmp.RecordsTotal - tmp.RecordsRemoved != sg_data.RecordsTotal - sg_data.RecordsRemoved {
					bs.NeedRecovery = "DC"
				}
			}
		}

		if len(bs.Group) != len(b.Meta.Groups) {
			bs.NeedRecovery = "WHOLE-GROUP"
		}

		reply.Buckets[b.Name] = bs
	}

	return
}

func (bctl *BucketCtl) ReadBucketConfig() error {
	data, err := ioutil.ReadFile(bctl.bucket_path)
	if err != nil {
		err = fmt.Errorf("Could not read bucket file '%s': %v", bctl.bucket_path, err)
		log.Printf("config: %v\n", err)
		return err
	}

	bctl.Lock()
	defer bctl.Unlock()

	bctl.Bucket = make([]*Bucket, 0, 0)

	for _, name := range strings.Split(string(data), "\n") {
		if len(name) > 0 {
			b, err := ReadBucket(bctl.e, name)
			if err != nil {
				log.Printf("config: could not read bucket: %s: %v\n", name, err)
				continue
			}

			bctl.Bucket = append(bctl.Bucket, b)
			log.Printf("config: new bucket: %s\n", b.Meta.String())
		}
	}

	if len(bctl.Bucket) == 0 {
		err = fmt.Errorf("No buckets found in bucket file '%s'", bctl.bucket_path)
		log.Printf("config: %v\n", err)
		return err
	}

	return nil
}

func (bctl *BucketCtl) ReadProxyConfig() error {
	conf := &config.ProxyConfig {}
	err := conf.Load(bctl.proxy_config_path)
	if err != nil {
		return fmt.Errorf("could not load proxy config file '%s': %v", bctl.proxy_config_path, err)
	}

	bctl.Lock()
	defer bctl.Unlock()

	bctl.Conf = conf

	return nil

}

func (bctl *BucketCtl) ReadConfig() error {
	err := bctl.ReadBucketConfig()
	if err != nil {
		return fmt.Errorf("failed to update bucket config: %v", err)
	}

	err = bctl.ReadProxyConfig()
	if err != nil {
		return fmt.Errorf("failed to update proxy config: %v", err)
	}

	return nil
}

func (bctl *BucketCtl) ReadBucketsMetaNolock(buckets []*Bucket) (new_buckets []*Bucket, err error) {
	new_buckets = make([]*Bucket, 0, len(buckets))

	for _, b := range buckets {
		rb, err := ReadBucket(bctl.e, b.Name)
		if err != nil {
			continue
		}

		new_buckets = append(new_buckets, rb)
	}

	if len(new_buckets) == 0 {
		new_buckets = nil
		err = fmt.Errorf("read-buckets-meta: could not read any bucket from %d requested", len(buckets))
		return
	}

	return
}

func (bctl *BucketCtl) ReadAllBucketsMeta() (err error) {
	var new_buckets, new_back_buckets []*Bucket

	bctl.RLock()
	if len(bctl.Bucket) != 0 {
		new_buckets, err = bctl.ReadBucketsMetaNolock(bctl.Bucket)
		if err != nil {
			log.Printf("read-all-buckets-meta: could not read buckets: %v\n", err)
		}
	}

	if len(bctl.BackBucket) != 0 {
		new_back_buckets, err = bctl.ReadBucketsMetaNolock(bctl.BackBucket)
		if err != nil {
			log.Printf("read-all-buckets-meta: could not read back buckets: %v\n", err)
		}
	}
	bctl.RUnlock()

	bctl.Lock()
	if new_buckets != nil {
		bctl.Bucket = new_buckets
	}

	if new_back_buckets != nil {
		bctl.BackBucket = new_back_buckets
	}
	bctl.Unlock()

	bctl.BucketStatUpdate()

	return nil
}

func NewBucketCtl(ell *etransport.Elliptics, bucket_path, proxy_config_path string) (bctl *BucketCtl, err error) {
	bctl = &BucketCtl {
		e:			ell,
		bucket_path:		bucket_path,
		proxy_config_path:	proxy_config_path,
		signals:		make(chan os.Signal, 1),

		Bucket:			make([]*Bucket, 0, 10),
		BackBucket:		make([]*Bucket, 0, 10),

		BucketTimer:		time.NewTimer(time.Second * 30),
		BucketStatTimer:	time.NewTimer(time.Second * 2),

		DefragTime:		time.Now(),
	}

	err = bctl.ReadConfig()
	if err != nil {
		return
	}
	bctl.BucketStatUpdate()

	signal.Notify(bctl.signals, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-bctl.BucketTimer.C:
				bctl.ReadAllBucketsMeta()

				if bctl.Conf.Proxy.BucketUpdateInterval > 0 {
					bctl.RLock()
					bctl.BucketTimer.Reset(time.Second * time.Duration(bctl.Conf.Proxy.BucketUpdateInterval))
					bctl.RUnlock()
				}

			case <-bctl.BucketStatTimer.C:
				bctl.BucketStatUpdate()

				if bctl.Conf.Proxy.BucketStatUpdateInterval > 0 {
					bctl.RLock()
					bctl.BucketStatTimer.Reset(time.Second * time.Duration(bctl.Conf.Proxy.BucketStatUpdateInterval))
					bctl.RUnlock()
				}

			case <-bctl.signals:
				bctl.ReadConfig()
				bctl.BucketStatUpdate()
			}
		}
	}()

	return bctl, nil
}
