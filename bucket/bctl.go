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
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	ProfilePath string = "backrunner.profile"

	// time to write 1 byte into error bucket in seconds
	// this is randomly selected error gain for buckets where upload has failed
	BucketWriteErrorPain float64	= 10000000000.0

	PainNoStats float64		= 15000000000.0
	PainStatError float64		= 15000000000.0
	PainStatRO float64		= 15000000000.0
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
		bctl.BackBucket = append(bctl.BackBucket, b)
		bucket = b
		bctl.Unlock()
	}

	return bucket, nil
}

func (bctl *BucketCtl) BucketStatUpdateNolock(stat *elliptics.DnetStat) (err error) {
	bctl.StatTime = stat.Time

	for _, b := range bctl.AllBuckets() {
		b.Group = make(map[uint32]*elliptics.StatGroup)

		for _, group := range b.Meta.Groups {
			sg, ok := stat.Group[group]
			if ok {
				b.Group[group] = sg
			}
		}
	}

	return
}

func (bctl *BucketCtl) BucketStatUpdate() (err error) {
	stat, err := bctl.e.Stat()
	if err != nil {
		return err
	}

	bctl.Lock()
	err = bctl.BucketStatUpdateNolock(stat)
	bctl.Unlock()

	// run defragmentation scan
	bctl.ScanBuckets()

	return err
}

func (bctl *BucketCtl) GetBucket(key string, req *http.Request) (bucket *Bucket) {
	s, err := bctl.e.MetadataSession()
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("get-bucket: could not create metadata session: %v", err))
		return bctl.Bucket[rand.Intn(len(bctl.Bucket))]
	}

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
			st, err := sg.FindStatBackendKey(s, key, group_id)
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

			if st.RO {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainStatRO
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

	var sum int64 = 0
	for {
		sum = 0
		var multiple int64 = 10

		for _, bs := range stat {
			sum += int64(bs.Range)
		}

		if sum >= multiple {
			break
		} else {
			for _, bs := range stat {
				bs.Range *= float64(multiple)
			}
		}
	}

	r := rand.Int63n(int64(sum))
	for _, bs := range stat {
		log.Printf("find-bucket: bucket: %s, pain: %f, range: %f, sum: %d, r: %d\n",
			bs.Bucket.Name, bs.Pain, bs.Range, sum, r)

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

	if total_size == 0 {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			"upload: attempting to perform invalid zero-length upload")
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
	// Let's consider our desired control point as number of useconds needed to write 1 byte into the storage
	// In the ideal world it would be zero

	time_us := time.Since(start).Nanoseconds() / 1000
	e := float64(time_us) / float64(total_size)

	bctl.Lock()

	for _, res := range reply.Servers {
		sg, ok := bucket.Group[res.Group]
		if ok {
			st, back_err := sg.FindStatBackend(res.Server, res.Backend)
			if back_err == nil {
				old_pain := st.PID.Pain
				update_pain := e
				estring := "ok"

				if res.Error != nil {
					update_pain = BucketWriteErrorPain
					estring = res.Error.Error()
				}
				st.PIDUpdate(update_pain)

				log.Printf("bucket-upload: bucket: %s, key: %s, size: %d, time: %d us, group: %d, e: %f, error: %v, pain: %f -> %f\n",
					bucket.Name, key, total_size, time_us, res.Group, e, estring, old_pain, st.PID.Pain)
			}
		}
	}

	if len(reply.SuccessGroups) == 0 {
		for _, group_id := range bucket.Meta.Groups {
			sg, ok := bucket.Group[group_id]
			if ok {
				st, back_err := sg.FindStatBackendKey(s, key, group_id)
				if back_err == nil {
					old_pain := st.PID.Pain

					log.Printf("bucket-upload: bucket: %s, key: %s, size: %d, time: %d us, error group: %d, e: %f, pain: %f -> %f\n",
						bucket.Name, key, total_size, time_us, group_id, e, old_pain, st.PID.Pain)
				}
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

	new_buckets := make([]*Bucket, 0, 0)

	for _, name := range strings.Split(string(data), "\n") {
		if len(name) > 0 {
			b, err := ReadBucket(bctl.e, name)
			if err != nil {
				log.Printf("config: could not read bucket: %s: %v\n", name, err)
				continue
			}

			new_buckets = append(new_buckets, b)
			log.Printf("config: new bucket: %s\n", b.Meta.String())
		}
	}

	if len(new_buckets) == 0 {
		err = fmt.Errorf("No buckets found in bucket file '%s'", bctl.bucket_path)
		log.Printf("config: %v\n", err)
		return err
	}

	bctl.Lock()
	bctl.Bucket = new_buckets
	bctl.Unlock()

	return nil
}

func (bctl *BucketCtl) ReadProxyConfig() error {
	conf := &config.ProxyConfig {}
	err := conf.Load(bctl.proxy_config_path)
	if err != nil {
		return fmt.Errorf("could not load proxy config file '%s': %v", bctl.proxy_config_path, err)
	}

	bctl.Lock()
	bctl.Conf = conf
	bctl.Unlock()

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

func (bctl *BucketCtl) ReadBucketsMetaNolock(names []string) (new_buckets []*Bucket, err error) {
	new_buckets = make([]*Bucket, 0, len(names))

	for _, name := range names {
		rb, err := ReadBucket(bctl.e, name)
		if err != nil {
			continue
		}

		new_buckets = append(new_buckets, rb)
	}

	if len(new_buckets) == 0 {
		new_buckets = nil
		err = fmt.Errorf("read-buckets-meta: could not read any bucket from %d requested", len(names))
		return
	}

	return
}

func (bctl *BucketCtl) ReadAllBucketsMeta() (err error) {
	bnames := make([]string, 0)
	bback_names := make([]string, 0)

	bctl.RLock()
	if len(bctl.Bucket) != 0 {
		for _, b := range bctl.Bucket {
			bnames = append(bnames, b.Name)
		}
	}
	if len(bctl.BackBucket) != 0 {
		for _, b := range bctl.BackBucket {
			bback_names = append(bnames, b.Name)
		}
	}
	bctl.RUnlock()


	new_buckets, err := bctl.ReadBucketsMetaNolock(bnames)
	if err != nil {
		log.Printf("read-all-buckets-meta: could not read buckets: %v\n", err)
	}
	new_back_buckets, err := bctl.ReadBucketsMetaNolock(bback_names)
	if err != nil {
		log.Printf("read-all-buckets-meta: could not read back buckets: %v\n", err)
	}

	stat, err := bctl.e.Stat()
	if err != nil {
		return err
	}

	bctl.Lock()
	if new_buckets != nil {
		bctl.Bucket = new_buckets
	}

	if new_back_buckets != nil {
		bctl.BackBucket = new_back_buckets
	}

	err = bctl.BucketStatUpdateNolock(stat)
	bctl.Unlock()

	return err
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
		BucketStatTimer:	time.NewTimer(time.Second * 10),

		DefragTime:		time.Now(),
	}

	runtime.SetBlockProfileRate(1000)

	err = bctl.ReadConfig()
	if err != nil {
		return
	}
	bctl.ReadAllBucketsMeta()

	signal.Notify(bctl.signals, syscall.SIGHUP)

	go func() {
		for {
			if len(bctl.Conf.Proxy.Root) != 0 {
				file, err := os.OpenFile(bctl.Conf.Proxy.Root + "/" + ProfilePath, os.O_RDWR | os.O_TRUNC | os.O_CREATE, 0644)
				if err != nil {
					return
				}

				fmt.Fprintf(file, "profile dump: %s\n", time.Now().String())
				pprof.Lookup("block").WriteTo(file, 2)

				file.Close()
			}

			time.Sleep(30 * time.Second)
		}
	}()

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
