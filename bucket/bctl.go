package bucket

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/range"
	"github.com/bioothod/backrunner/reply"
	"github.com/bioothod/elliptics-go/elliptics"
	"io"
	"io/ioutil"
	"log"
	//"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
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

	PainNoFreeSpaceSoft float64	= 5000000000.0
	PainNoFreeSpaceHard float64	= 50000000000000.0

	// pain for read-only groups
	PainStatRO float64		= PainNoFreeSpaceHard / 2

	// this is randomly selected error gain for buckets where upload has failed
	BucketWriteErrorPain float64	= PainNoFreeSpaceHard / 2

	// pain for group without statistics
	PainNoStats float64		= PainNoFreeSpaceHard / 2

	// pain for group where statistics contains error field
	PainStatError float64		= PainNoFreeSpaceHard / 2

	// pain for bucket which do not have its group in stats
	PainNoGroup float64		= PainNoFreeSpaceHard / 2

	PainDiscrepancy float64		= 1000.0
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
	// after config has been locally updated via HTTP request,
	// it disables automatic config update for time period specified in config
	DisableConfigUpdateUntil	time.Time

	signals			chan os.Signal

	BucketTimer		*time.Timer
	BucketStatTimer		*time.Timer

	// time when previous statistics update has been performed
	StatTime		time.Time

	// time when backrunner proxy started
	StartTime		time.Time

	// time when the last time config update was done
	ConfigTime		time.Time

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
		for _, group := range b.Meta.Groups {
			sg, ok := stat.Group[group]
			if ok {
				b.Group[group] = sg
			} else {
				log.Printf("bucket-stat-update: bucket: %s, group: %d: there is no bucket stat, using old values (if any)",
					b.Name, group)
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

	return err
}

func FreeSpaceRatio(st *elliptics.StatBackend, content_length uint64) float64 {
	free_space_rate := 1.0 - float64(st.VFS.BackendUsedSize + content_length) / float64(st.VFS.TotalSizeLimit)
	if st.VFS.Avail <= st.VFS.TotalSizeLimit {
		if st.VFS.Avail < content_length {
			free_space_rate = 0
		} else {
			free_space_rate = float64(st.VFS.Avail - content_length) / float64(st.VFS.TotalSizeLimit)
		}
	}

	return free_space_rate
}

func (bctl *BucketCtl) GetBucket(key string, req *http.Request) (bucket *Bucket) {
	s, err := bctl.e.MetadataSession()
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("get-bucket: could not create metadata session: %v", err))
		return bctl.Bucket[rand.Intn(len(bctl.Bucket))]
	}
	defer s.Delete()

	type bucket_stat struct {
		Bucket		*Bucket
		SuccessGroups	[]uint32
		ErrorGroups	[]uint32
		Pain		float64
		Range		float64

		pains		[]float64
		free_rates	[]float64

		abs		[]string
	}

	stat := make([]*bucket_stat, 0)
	failed := make([]*bucket_stat, 0)

	bctl.RLock()

	for _, b := range bctl.Bucket {
		bs := &bucket_stat {
			Bucket:		b,
			SuccessGroups:	make([]uint32, 0),
			ErrorGroups:	make([]uint32, 0),
			Pain:		0.0,
			Range:		0.0,

			pains:		make([]float64, 0, len(b.Group)),
			free_rates:	make([]float64, 0, len(b.Group)),

		}

		s.SetNamespace(b.Name)

		for group_id, sg := range b.Group {
			_, _, err := s.LookupBackend(key, group_id)
			if err != nil {
				// there is no route to the requested group, penalize this bucket
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoStats
				continue
			}

			st, err := sg.FindStatBackendKey(s, key, group_id)
			if err != nil {
				// there is no statistics for given address+backend, which should host our data
				// do not allow to write into the bucket which contains given address+backend

				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoStats
				continue
			}

			bs.abs = append(bs.abs, st.Ab.String())

			if st.RO {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainStatRO
				continue
			}

			if st.Error.Code != 0 {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainStatError
				continue
			}

			// this is an empty stat structure
			if st.VFS.TotalSizeLimit == 0 || st.VFS.Total  == 0 {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoStats
				continue
			}

			free_space_rate := FreeSpaceRatio(st, uint64(req.ContentLength))
			bs.Pain += 1000.0 / free_space_rate * 5.0

			if free_space_rate <= bctl.Conf.Proxy.FreeSpaceRatioHard {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoFreeSpaceHard
			} else if free_space_rate <= bctl.Conf.Proxy.FreeSpaceRatioSoft {
				bs.ErrorGroups = append(bs.ErrorGroups, group_id)

				bs.Pain += PainNoFreeSpaceSoft
			} else {
				bs.SuccessGroups = append(bs.SuccessGroups, group_id)
			}

			pp := st.PIDPain()

			bs.Pain += pp * float64(req.ContentLength)
			bs.pains = append(bs.pains, pp)
			bs.free_rates = append(bs.free_rates, free_space_rate)
		}

		total_groups := len(bs.SuccessGroups) + len(bs.ErrorGroups)
		diff := 0
		if len(b.Meta.Groups) > total_groups {
			diff += len(b.Meta.Groups) - total_groups
		}

		bs.Pain += float64(diff) * PainNoGroup

		// calculate discrepancy pain:
		// run over all address+backends in every group in given bucket,
		// sum up number of live records
		// set discrepancy as a maximum difference between number of records among all groups
		var min_records uint64 = 1<<31-1
		var max_records uint64 = 0

		records := make([]uint64, 0)
		for _, sg := range b.Group {
			var r uint64 = 0

			for _, sb := range sg.Ab {
				r += sb.VFS.RecordsTotal - sb.VFS.RecordsRemoved
			}

			records = append(records, r)
		}

		for _, r := range records {
			if r < min_records {
				min_records = r
			}

			if r > max_records {
				max_records = r
			}
		}
		bs.Pain += float64(max_records - min_records) * PainDiscrepancy


		// do not even consider buckets without free space even in one group
		if bs.Pain >= PainNoFreeSpaceHard {
			//log.Printf("find-bucket: url: %s, bucket: %s, content-length: %d, " +
			//	"groups: %v, success-groups: %v, error-groups: %v, " +
			//	"pain: %f, pains: %v, free_rates: %v: pain is higher than HARD limit\n",
			//	req.URL.String(), b.Name, req.ContentLength, b.Meta.Groups, bs.SuccessGroups, bs.ErrorGroups, bs.Pain,
			//	bs.pains, bs.free_rates)
			failed = append(failed, bs)
			continue
		}

		if bs.Pain != 0 {
			bs.Range = 1.0 / bs.Pain
		} else {
			bs.Range = 1.0
		}

		stat = append(stat, bs)
	}

	bctl.RUnlock()

	// there are no buckets suitable for this request
	// either there is no space in either bucket, or there are no buckets at all
	if len(stat) == 0 {
		str := make([]string, 0)
		for _, bs := range failed {
			str = append(str,
				fmt.Sprintf("{bucket: %s, success-groups: %v, error-groups: %v, groups: %v, " +
						"abs: %v, pain: %f, free-rates: %v}",
						bs.Bucket.Name, bs.SuccessGroups, bs.ErrorGroups, bs.Bucket.Meta.Groups,
						bs.abs, bs.Pain, bs.free_rates))
		}

		log.Printf("find-bucket: url: %s, content-length: %d: there are no suitable buckets: %v",
			req.URL.String(), req.ContentLength, str)
		return nil
	}

	// get rid of buckets without free space if we do have other buckets
	ok_buckets := 0
	nospace_buckets := 0
	for _, bs := range stat {
		if bs.Pain < PainNoFreeSpaceSoft {
			ok_buckets++
		} else {
			nospace_buckets++
		}
	}

	if nospace_buckets != 0 && ok_buckets != 0 {
		tmp := make([]*bucket_stat, 0)
		for _, bs := range stat {
			if bs.Pain < PainNoFreeSpaceSoft {
				tmp = append(tmp, bs)
			}
		}

		stat = tmp
	}

	str := make([]string, 0)
	show_num := 0
	for _, bs := range stat {
		str = append(str,
			fmt.Sprintf("{bucket: %s, success-groups: %v, error-groups: %v, groups: %v, " +
					"abs: %v, pain: %f, free-rates: %v}",
					bs.Bucket.Name, bs.SuccessGroups, bs.ErrorGroups, bs.Bucket.Meta.Groups,
					bs.abs, bs.Pain, bs.free_rates))

		if show_num >= 5 {
			break
		}
	}

	log.Printf("find-bucket: url: %s, content-length: %d, buckets: %d, showing top %d: %v",
		req.URL.String(), req.ContentLength, len(stat), len(str), str)

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
		r -= int64(bs.Range)
		if r <= 0 {
			log.Printf("find-bucket: url: %s, selected bucket: %s, content-length: %d, groups: %v, success-groups: %v, error-groups: %v, pain: %f, pains: %v, free_rates: %v\n",
				req.URL.String(), bs.Bucket.Name, req.ContentLength,
				bs.Bucket.Meta.Groups, bs.SuccessGroups, bs.ErrorGroups,
				bs.Pain, bs.pains, bs.free_rates)
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
	defer s.Delete()

	s.SetFilter(elliptics.SessionFilterAll)
	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)
	s.SetTimeout(100)
	s.SetIOflags(elliptics.IOflag(bctl.Conf.Proxy.WriterIOFlags))

	log.Printf("upload-trace-id: %x: url: %s, bucket: %s, key: %s, id: %s\n",
		s.GetTraceID(), req.URL.String(), bucket.Name, key, s.Transform(key))

	ranges, err := ranges.ParseRange(req.Header.Get("Range"), int64(total_size))
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("upload: %v", err))
		return
	}

	var offset uint64 = 0
	if len(ranges) != 0 {
		offset = uint64(ranges[0].Start)
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

	bctl.RLock()

	str := make([]string, 0)
	for _, res := range reply.Servers {
		sg, ok := bucket.Group[res.Group]
		if ok {
			st, back_err := sg.FindStatBackend(res.Addr, res.Backend)
			if back_err == nil {
				old_pain := st.PIDPain()
				update_pain := e
				estring := "ok"

				if res.Error != nil {
					update_pain = BucketWriteErrorPain
					estring = res.Error.Error()
				}
				st.PIDUpdate(update_pain)

				str = append(str, fmt.Sprintf("{group: %d, time: %d us, e: %f, error: %v, pain: %f -> %f}",
					res.Group, time_us, e, estring, old_pain, st.PIDPain()))
			} else {
				str = append(str, fmt.Sprintf("{group: %d, time: %d us, e: %f, error: no backend stat}",
					res.Group, time_us, e))
			}
		} else {
			str = append(str, fmt.Sprintf("{group: %d, time: %d us, e: %f, error: no group stat}",
				res.Group, time_us, e))
		}
	}

	if len(reply.SuccessGroups) == 0 {
		for _, group_id := range bucket.Meta.Groups {
			str = append(str, fmt.Sprintf("{error-group: %d, time: %d us}", group_id, time_us))
		}
	}

	bctl.RUnlock()

	log.Printf("bucket-upload: bucket: %s, key: %s, size: %d: %v\n", bucket.Name, key, total_size, str)

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

func (bctl *BucketCtl) SetContentType(key string, w http.ResponseWriter) {
	bctl.RLock()
	defer bctl.RUnlock()

	for k, v := range bctl.Conf.Proxy.ContentTypes {
		if strings.HasSuffix(key, k) {
			w.Header().Set("Content-Type", v)
			return
		}
	}

	return
}

func (bctl *BucketCtl) SetGroupsTimeout(s *elliptics.Session, bucket *Bucket, key string) {
	// sort groups by defrag state, increase timeout if needed

	groups := make([]uint32, 0)
	defrag_groups := make([]uint32, 0)
	timeout := 30

	for group_id, sg := range bucket.Group {
		sb, err := sg.FindStatBackendKey(s, key, group_id)
		if err != nil {
			continue
		}

		if sb.DefragState != 0 {
			defrag_groups = append(defrag_groups, group_id)
		} else {
			groups = append(groups, group_id)
		}
	}

	// Not being defragmented backends first, then those which are currently being defragmented
	groups = append(groups, defrag_groups...)
	if len(groups) == len(defrag_groups) {
		timeout = 90
	}

	// if there are no backends being defragmented, use weights to mix read states
	// if there are such backends, use strict order and read from non-defragmented backends first
	ioflags := elliptics.IOflag(bctl.Conf.Proxy.ReaderIOFlags) | s.GetIOflags()
	if len(defrag_groups) == 0 {
		ioflags |= elliptics.DNET_IO_FLAGS_MIX_STATES
	}
	s.SetIOflags(ioflags)

	// there are no stats for bucket groups, use what we have in metadata
	if len(groups) == 0 {
		groups = bucket.Meta.Groups
	}

	s.SetGroups(groups)
	s.SetTimeout(timeout)
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
	defer s.Delete()

	s.SetFilter(elliptics.SessionFilterAll)
	s.SetNamespace(bucket.Name)
	bctl.SetGroupsTimeout(s, bucket, key)

	log.Printf("stream-trace-id: %x: url: %s, bucket: %s, key: %s, id: %s\n",
		s.GetTraceID(), req.URL.String(), bucket.Name, key, s.Transform(key))

	offset, size, err := URIOffsetSize(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("stream: %v", err))
		return
	}

	if offset != 0 || size != 0 {
		if size == 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset + size - 1))
		}
	}

	rs, err := elliptics.NewReadSeekerOffsetSize(s, key, offset, size)
	if err != nil {
		err = errors.NewKeyErrorFromEllipticsError(err, req.URL.String(), "stream: could not create read-seeker")
		return
	}
	defer rs.Free()

	bctl.SetContentType(key, w)
	http.ServeContent(w, req, key, rs.Mtime, rs)
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
	defer s.Delete()

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)
	s.SetIOflags(elliptics.IOflag(bctl.Conf.Proxy.ReaderIOFlags))

	log.Printf("lookup-trace-id: %x: url: %s, bucket: %s, key: %s, id: %s\n",
		s.GetTraceID(), req.URL.String(), bucket.Name, key, s.Transform(key))

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
	defer s.Delete()

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)
	s.SetIOflags(elliptics.IOflag(bctl.Conf.Proxy.WriterIOFlags))

	log.Printf("delete-trace-id: %x: url: %s, bucket: %s, key: %s, id: %s\n",
		s.GetTraceID(), req.URL.String(), bucket.Name, key, s.Transform(key))

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
	defer s.Delete()

	s.SetNamespace(bucket.Name)
	s.SetGroups(bucket.Meta.Groups)
	s.SetIOflags(elliptics.IOflag(bctl.Conf.Proxy.WriterIOFlags))

	log.Printf("bulk-delete-trace-id: %x: url: %s, bucket: %s, keys: %v\n",
		s.GetTraceID(), req.URL.String(), bucket.Name, keys)

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
}

type BctlStat struct {
	Buckets		map[string]*BucketStat
	StatTime	string
}

func (bctl *BucketCtl) Stat(req *http.Request, bnames []string) (reply *BctlStat, err error) {
	bctl.RLock()
	defer bctl.RUnlock()

	reply = &BctlStat {
		Buckets:		make(map[string]*BucketStat),
		StatTime:		bctl.StatTime.String(),
	}

	for _, b := range bctl.AllBuckets() {
		if len(bnames) != 0 {
			found := false
			for _, name := range(bnames) {
				if b.Name == name {
					found = true
					break
				}
			}

			if !found {
				continue
			}
		}

		bs := &BucketStat {
			Group:	make(map[string]*elliptics.StatGroupData),
			Meta:	&b.Meta,
		}

		for group, sg := range b.Group {
			sg_data := sg.StatGroupData()
			bs.Group[fmt.Sprintf("%d", group)] = sg_data
		}

		reply.Buckets[b.Name] = bs
	}

	return
}

func (bctl *BucketCtl) EllipticsReadBucketList() (data []byte, err error) {
	ms, err := bctl.e.MetadataSession()
	if err != nil {
		return
	}
	defer ms.Delete()

	ms.SetNamespace(BucketNamespace)

	for rd := range ms.ReadData(bctl.Conf.Elliptics.BucketList, 0, 0) {
		if rd.Error() != nil {
			err = rd.Error()

			log.Printf("elliptics-read-bucket-list: %s: could not read bucket list: %v", bctl.Conf.Elliptics.BucketList, err)
			return
		}

		data = rd.Data()
		return
	}

	err = fmt.Errorf("elliptics-read-bucket-list: %s: could not read bucket list: ReadData() returned nothing",
			bctl.Conf.Elliptics.BucketList)
	log.Printf(err.Error())
	return
}


func (bctl *BucketCtl) ReadBucketConfig() (err error) {
	var data []byte

	if len(bctl.Conf.Elliptics.BucketList) != 0 {
		data, err = bctl.EllipticsReadBucketList()
		if err == nil {
			log.Printf("Successfully read bucket list from: %s\n", bctl.Conf.Elliptics.BucketList)
		}
	}


	if err != nil || data == nil || len(data) == 0 || len(bctl.Conf.Elliptics.BucketList) == 0 {
		data, err = ioutil.ReadFile(bctl.bucket_path)
		if err != nil {
			err = fmt.Errorf("Could not read bucket file '%s': %v", bctl.bucket_path, err)
			log.Printf("config: %v\n", err)
			return err
		}
	}

	new_buckets := make([]*Bucket, 0, 0)
	new_back_buckets := make([]*Bucket, 0, 0)

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

	back_names := make([]string, 0)
	bctl.Lock()
	for _, back_bucket := range bctl.BackBucket {
		back_names = append(back_names, back_bucket.Name)
	}
	bctl.Unlock()

	for _, name := range back_names {
		b, err := ReadBucket(bctl.e, name)
		if err != nil {
			log.Printf("config: could not read bucket: %s: %v\n", name, err)
			continue
		}

		new_back_buckets = append(new_back_buckets, b)
		log.Printf("config: new back bucket: %s\n", b.Meta.String())
	}

	stat, err := bctl.e.Stat()
	if err != nil {
		return err
	}

	bctl.Lock()
	bctl.Bucket = new_buckets
	bctl.BackBucket = new_back_buckets
	err = bctl.BucketStatUpdateNolock(stat)
	bctl.Unlock()

	log.Printf("Bucket config has been updated, there are %d writable buckets and %d back buckets\n",
		len(new_buckets), len(new_back_buckets))
	return nil
}

func (bctl *BucketCtl) EllipticsReadBackrunnerConfig(conf *config.ProxyConfig, conf_key string) (err error) {
	ms, err := bctl.e.MetadataSession()
	if err != nil {
		return
	}
	defer ms.Delete()

	ms.SetNamespace(BucketNamespace)

	for rd := range ms.ReadData(conf_key, 0, 0) {
		if rd.Error() != nil {
			err = rd.Error()

			log.Printf("elliptics-read-backrunner-config: %s: could not read backrunner config: %v",
				conf_key, err)
			return
		}

		reader := bytes.NewReader(rd.Data())
		err = conf.LoadIO(reader)
		if err != nil {
			log.Printf("elliptics-read-backrunner-config: %s: could not load config from elliptics data: %v",
				conf_key, err)
			return
		}

		return
	}

	err = fmt.Errorf("elliptics-read-backrunner-config: %s: could not read backrunner config: ReadData() returned nothing",
			conf_key)
	log.Printf(err.Error())
	return
}

func (bctl *BucketCtl) ReadProxyConfig() error {
	conf := &config.ProxyConfig {}
	err := conf.Load(bctl.proxy_config_path)
	if err != nil {
		return fmt.Errorf("could not load proxy config file '%s': %v", bctl.proxy_config_path, err)
	}

	if len(conf.Elliptics.BackrunnerConfig) != 0 {
		ell_conf := &config.ProxyConfig {}
		err = bctl.EllipticsReadBackrunnerConfig(ell_conf, conf.Elliptics.BackrunnerConfig)
		if err == nil {
			log.Printf("Successfully read backrunner config from: %s\n", conf.Elliptics.BackrunnerConfig)
			conf = ell_conf
		}
	}

	if err != nil || len(conf.Elliptics.BackrunnerConfig) == 0 {
		err = conf.Load(bctl.proxy_config_path)
		if err != nil {
			return fmt.Errorf("could not load proxy config file '%s': %v", bctl.proxy_config_path, err)
		}
	}

	if time.Now().Before(bctl.DisableConfigUpdateUntil) {
		log.Printf("Proxy config has been read, but automatic config update is disabled until %s\n",
			bctl.DisableConfigUpdateUntil.String())
		return nil
	}

	bctl.Lock()
	bctl.Conf = conf
	bctl.Unlock()

	if bctl.e.LogFile != nil {
		bctl.e.LogFile.Close()
	}

	bctl.e.LogFile, err = os.OpenFile(conf.Elliptics.LogFile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Could not open log file '%s': %q", conf.Elliptics.LogFile, err)
	}

	log.SetPrefix(conf.Elliptics.LogPrefix)
	log.SetOutput(bctl.e.LogFile)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Printf("Proxy config has been updated\n")
	return nil

}

func (bctl *BucketCtl) UpdateMetadata(key string, jsi interface{}) (err error) {
	ms, err := bctl.e.MetadataSession()
	if err != nil {
		log.Printf("%s: metadata update: could not create metadata session: %v", key, err)
		return
	}
	defer ms.Delete()

	ms.SetNamespace(BucketNamespace)

	type Meta struct {
		Key string
		Timestamp int64
	}

	js := struct {
		Meta Meta
		Data interface{}
	} {
		Meta {
			key,
			time.Now().Unix(),
		},
		jsi,
	}

	data, err := json.MarshalIndent(&js, "", "  ")
	if err != nil {
		log.Printf("%s: metadata update: could not pack json: %v: %v", key, js, err)
		return
	}

	for wr := range ms.WriteData(key, bytes.NewReader(data), 0, 0) {
		if wr.Error() != nil {
			err = wr.Error()

			log.Printf("%s: metadata update: could not write data: %v", key, err)
		}
	}

	return
}
type BucketCtlStat struct {
	StartTime		int64
	StartTimeString		string

	StatTime		int64
	StatTimeString		string

	ConfigTime		int64
	ConfigTimeString	string

	CurrentTime		int64
	CurrentTimeString	string

	BucketNum		int
	Hostname		string
	ConfigUpdateInterval	int
	StatUpdateInterval	int
	BuildDate		string
	LastCommit		string
	EllipticsGoLastCommit	string

	ProxyConfig		config.ProxyConfig
}

func (bctl *BucketCtl) NewBucketCtlStat() (*BucketCtlStat) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("new-bucket-ctl: hostname error: %v", err)
		hostname = ""
	}

	ctl := &BucketCtlStat {
		StartTime:		bctl.StartTime.Unix(),
		StartTimeString:	bctl.StartTime.String(),

		StatTime:		bctl.StatTime.Unix(),
		StatTimeString:		bctl.StatTime.String(),

		ConfigTime:		bctl.ConfigTime.Unix(),
		ConfigTimeString:	bctl.ConfigTime.String(),

		CurrentTime:		time.Now().Unix(),
		CurrentTimeString:	time.Now().String(),

		BucketNum:		len(bctl.Bucket),
		Hostname:		hostname,
		ConfigUpdateInterval:	bctl.Conf.Proxy.BucketUpdateInterval,
		StatUpdateInterval:	bctl.Conf.Proxy.BucketStatUpdateInterval,
		BuildDate:		config.BuildDate,
		LastCommit:		config.LastCommit,
		EllipticsGoLastCommit:	config.EllipticsGoLastCommit,

		ProxyConfig:		*bctl.Conf,
	}

	// do not show secure tokens
	ctl.ProxyConfig.Proxy.RedirectToken = ""

	return ctl
}

func (bctl *BucketCtl) ReadConfig() (err error) {
	err = bctl.ReadProxyConfig()
	if err != nil {
		err = fmt.Errorf("read-config: failed to update proxy config: %v", err)
		log.Printf("%s", err)
		return
	}

	err = bctl.ReadBucketConfig()
	if err != nil {
		err = fmt.Errorf("read-config: failed to update bucket config: %v", err)
		log.Printf("%s", err)
		return
	}

	bctl.ConfigTime = time.Now()

	ctl := bctl.NewBucketCtlStat()

	err = bctl.UpdateMetadata(fmt.Sprintf("%s.ReadConfig", ctl.Hostname), ctl)
	if err == nil {
		log.Printf("read-config: updated metadata: hostname: %s, build-data: %s, last-commit: %s, elliptics-go-last-commit: %s\n",
			ctl.Hostname, config.BuildDate, config.LastCommit, config.EllipticsGoLastCommit)
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

func (bctl *BucketCtl) DumpProfileFile(add_time bool) {
	if len(bctl.Conf.Proxy.Root) != 0 {
		profile := ProfilePath
		if add_time {
			profile += "-" + time.Now().Format("2006.01.02-15:04:05.000")
		}

		p := path.Join(bctl.Conf.Proxy.Root, profile)
		file, err := os.OpenFile(p, os.O_RDWR | os.O_TRUNC | os.O_CREATE, 0644)
		if err != nil {
			log.Printf("dump-profile: failed to open profile '%s': %v", p, err)
			return
		}
		defer file.Close()

		types := []string{"goroutine", "heap", "threadcreate"}
		bctl.DumpProfile(file, types)
	}
}

func (bctl *BucketCtl) DumpProfileSingle(out io.Writer, name string) {
	data := pprof.Lookup(name)
	if data != nil {
		data.WriteTo(out, 2)
	}
}

func (bctl *BucketCtl) DumpProfile(out io.Writer, types []string) {
	for _, t := range types {
		bctl.DumpProfileSingle(out, t)
	}
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

		StartTime:		time.Now(),
	}

	runtime.SetBlockProfileRate(1000)

	err = bctl.ReadConfig()
	if err != nil {
		return
	}

	signal.Notify(bctl.signals, syscall.SIGHUP)

	go func() {
		for {
			bctl.DumpProfileFile(false)
			time.Sleep(30 * time.Second)
		}
	}()

	go func() {
		for {
			select {
			case <-bctl.BucketTimer.C:
				bctl.ReadConfig()

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
				// reread config and clean back/read-only bucket list
				bctl.ReadConfig()

				bctl.Lock()
				bctl.BackBucket = make([]*Bucket, 0, 10)
				bctl.Unlock()
			}
		}
	}()

	return bctl, nil
}
