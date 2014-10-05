package bucket

import (
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/elliptics-go/elliptics"
	"fmt"
	"io/ioutil"
	"log"
	//"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
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
	e		*etransport.Elliptics

	Ticker		*time.Ticker

	sync.RWMutex
	DnetStat	*elliptics.DnetStat

	// buckets used for automatic write bucket selection,
	// i.e. when client doesn't provide bucket name and we select it
	// according to its performance and capacity
	Bucket		[]*Bucket

	// buckets used by clients directly, i.e. when client explicitly says
	// he wants to work with bucket named 'X'
	BackBucket	[]*Bucket
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

	if bctl.DnetStat != stat {
		bctl.Lock()
		defer bctl.Unlock()

		if bctl.DnetStat != stat {
			succeed_groups := make([]uint32, 0)
			failed_groups := make([]uint32, 0)

			for _, b := range bctl.AllBuckets() {
				b.Group = make(map[uint32]*elliptics.StatGroup)

				for _, group := range b.Meta.Groups {
					sg, ok := stat.Group[group]
					if !ok {
						failed_groups = append(failed_groups, group)
					} else {
						b.Group[group] = sg
						succeed_groups = append(succeed_groups, group)
					}
				}
			}

			bctl.DnetStat = stat
			log.Printf("bctl: stats have been updated: succeed-groups: %v, failed-gropus: %v\n",
				succeed_groups, failed_groups)
		}
	}

	return
}

func (bctl *BucketCtl) GetBucket() (bucket *Bucket) {
	bctl.RLock()
	defer bctl.RUnlock()

	return bctl.Bucket[rand.Intn(len(bctl.Bucket))]
}

func (bctl *BucketCtl) bucket_upload(bucket *Bucket, key string, req *http.Request) (reply map[string]interface{}, err error) {
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

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)
	s.SetTimeout(100)

	offset, _, err := URIOffsetSize(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("upload: %v", err))
		return
	}

	reply, err = bucket.lookup_serialize(true, s.WriteData(key, req.Body, offset, total_size))
	return
}

func (bctl *BucketCtl) Upload(key string, req *http.Request) (reply map[string]interface{}, bucket *Bucket, err error) {
	bucket = bctl.GetBucket()

	reply, err = bctl.bucket_upload(bucket, key, req)
	return
}

func (bctl *BucketCtl) BucketUpload(bucket_name, key string, req *http.Request) (reply map[string]interface{}, bucket *Bucket, err error) {
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


func (bctl *BucketCtl) Lookup(bname, key string, req *http.Request) (reply map[string]interface{}, err error) {
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

func (bctl *BucketCtl) Stat(req *http.Request) (reply map[string]interface{}, err error) {
	reply = make(map[string]interface{})

	buckets := make(map[string]interface{})

	bctl.RLock()
	defer bctl.RUnlock()

	for _, b := range bctl.AllBuckets() {
		buckets[b.Name], err = b.Stat()
		if err != nil {
			buckets[b.Name] = err_struct {
				Error: err.Error(),
			}
			err = nil
			return
		}
	}

	reply["buckets"] = buckets

	return
}

func NewBucketCtl(ell *etransport.Elliptics, bucket_path string) (bctl *BucketCtl, err error) {
	bctl = &BucketCtl {
		e:		ell,
		DnetStat:	nil,
		Ticker:		time.NewTicker(time.Second * 10),
		Bucket:		make([]*Bucket, 0, 10),
	}

	data, err := ioutil.ReadFile(bucket_path)
	if err != nil {
		return
	}

	for _, name := range strings.Split(string(data), "\n") {
		if len(name) > 0 {
			b, err := ReadBucket(bctl.e, name)
			if err != nil {
				log.Printf("bucket-ctl: could not read bucket: %s: %v\n", name, err)
				continue
			}

			bctl.Bucket = append(bctl.Bucket, b)
		}
	}

	if len(bctl.Bucket) == 0 {
		log.Fatal("No buckets found in bucket file")
	}

	bctl.BucketStatUpdate()

	go func() {
		for _ = range bctl.Ticker.C {
			bctl.BucketStatUpdate()
		}
	}()

	return bctl, nil
}
