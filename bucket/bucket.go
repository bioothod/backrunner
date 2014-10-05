package bucket

import (
	"bytes"
	"encoding/json"
	"encoding/hex"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/elliptics-go/elliptics"
	"github.com/vmihailenco/msgpack"
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

type err_struct struct {
	Error string
}


const BucketNamespace string = "bucket"

type BucketACL struct {
	Version int32	`json:"-"`
	User    string	`json:"user"`
	Token   string	`json:"token"`
	Flags   uint64	`json:"flags"`
}

const (
	// a placeholder for /get/ request which doesn't enforce additional checks besides auth check,
	// i.e. it is not admin, it is not modification
	BucketAuthEmpty		uint64		= 0

	// when ACL contains this flag, no further auth checks are ever performed for given user
	BucketAuthNoToken	uint64		= 1

	// ACL must contain this flag to allow user to upload data
	BucketAuthWrite		uint64		= 2

	// currently unused ACL flag which was introduced to split admin role (bucket modification) from usual writers
	// it is unused since backrunner doesn't support bucket modification or creation,
	// there is special tool @bmeta for this
	BucketAuthAdmin		uint64		= 4
)

type BucketMsgpack struct {
	Version     int32			`json:"-"`
	Name        string			`json:"-"`
	Acl         map[string]BucketACL	`json:"-"`
	Groups      []uint32			`json:"groups"`
	Flags       uint64			`json:"flags"`
	MaxSize     uint64			`json:"max-size"`
	MaxKeyNum   uint64			`json:"max-key-num"`
	reserved    [3]uint64			`json:"-"`
}

func (meta *BucketMsgpack) PackMsgpack() (interface{}, error) {
	var out []interface{} = make([]interface{}, 10, 10)

	out[0] = meta.Version
	out[1] = meta.Name

	var acls map[interface{}]interface{} = make(map[interface{}]interface{})
	for _, acl := range meta.Acl {
		var one_acl []interface{} = make([]interface{}, 4, 4)
		one_acl[0] = acl.Version
		one_acl[1] = acl.User
		one_acl[2] = acl.Token
		one_acl[3] = acl.Flags

		acls[acl.User] = one_acl
	}
	out[2] = acls

	var groups []interface{}
	for _, g := range meta.Groups {
		groups = append(groups, g)
	}
	out[3] = groups

	out[4] = meta.Flags
	out[5] = meta.MaxSize
	out[6] = meta.MaxKeyNum

	for i, r := range meta.reserved {
		out[7 + i] = r
	}

	return out, nil
}

func (meta *BucketMsgpack) ExtractMsgpack(out []interface{}) (err error) {
	if len(out) < 10 {
		return fmt.Errorf("array length: %d, must be at least 10", len(out))
	}
	meta.Version = int32(out[0].(int64))
	if meta.Version != 1 {
		return fmt.Errorf("unsupported metadata version %d", meta.Version)
	}
	meta.Name = out[1].(string)

	meta.Acl = make(map[string]BucketACL)
	for _, i := range out[2].(map[interface{}]interface{}) {
		x := i.([]interface{})
		var acl BucketACL
		if v, ok := x[0].(int64); ok {
			acl.Version = int32(v)
		} else {
			return fmt.Errorf("acl: could not find version")
		}
		if v, ok := x[1].(string); ok {
			acl.User = v
		} else {
			return fmt.Errorf("acl: could not find user")
		}
		if v, ok := x[2].(string); ok {
			acl.Token = v
		} else {
			return fmt.Errorf("acl: could not find token")
		}
		if v, ok := x[3].(int64); ok {
			acl.Flags = uint64(v)
		} else {
			return fmt.Errorf("acl: could not find flags")
		}

		meta.Acl[acl.User] = acl
	}

	for _, x := range out[3].([]interface{}) {
		meta.Groups = append(meta.Groups, uint32(x.(int64)))
	}
	meta.Flags = uint64(out[4].(int64))
	meta.MaxSize = uint64(out[5].(int64))
	meta.MaxKeyNum = uint64(out[6].(int64))

	for i := range meta.reserved {
		meta.reserved[i] = uint64(out[7 + i].(int64))
	}

	return nil
}

type Bucket struct {
	Name	string
	Group	map[uint32]*elliptics.StatGroup

	Meta	BucketMsgpack

	Time    time.Time
}

func (b *Bucket) Stat() (reply map[string]interface{}, err error) {
	reply = make(map[string]interface{})

	g := make(map[string]interface{})
	for group, stat := range b.Group {
		g[fmt.Sprintf("%d", group)] = stat.StatGroupData()
	}
	reply["groups"] = g
	reply["meta"] = b.Meta

	err = nil
	return
}

type BucketCtl struct {
	e		*etransport.Elliptics

	Ticker		*time.Ticker

	sync.RWMutex
	DnetStat	*elliptics.DnetStat
	Bucket		[]*Bucket
}

func (bctl *BucketCtl) FindBucket(name string) (bucket *Bucket, err error) {
	bctl.RLock()
	defer bctl.RUnlock()

	for _, b := range bctl.Bucket {
		if b.Name == name {
			bucket = b
			err = nil
			return
		}
	}

	bucket = nil
	err = fmt.Errorf("%s: could not find bucket", name)
	return
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

			for _, b := range bctl.Bucket {
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

func (b *Bucket) check_auth(r *http.Request, required_flags uint64) (err error) {
	if len(b.Meta.Acl) == 0 {
		err = nil
		return
	}

	user, recv_auth, err := auth.GetAuthInfo(r)
	if err != nil {
		return
	}

	acl, ok := b.Meta.Acl[user]
	if !ok {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("auth: header: '%v': there is no user '%s' in ACL",
				r.Header[auth.AuthHeaderStr], user))
		return
	}

	log.Printf("check-auth: user: %s, token: %s, flags: %x, required: %x\n", acl.User, acl.Token, acl.Flags, required_flags)

	// only require required_flags check if its not @BucketAuthEmpty
	// @BucketAuthEmpty required_flags is set by reader, non BucketAuthEmpty required_flags are supposed to mean modifications
	if required_flags != BucketAuthEmpty {
		// there are no required flags in ACL
		if (acl.Flags & required_flags) == 0 {
			err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
				fmt.Sprintf("auth: header: '%v': user '%s' is not allowed to do action: acl-flags: 0x%x, required-flags: 0x%x",
					r.Header[auth.AuthHeaderStr], user, acl.Flags, required_flags))
			return
		}
	}

	// skip authorization if special ACL flag is set
	if (acl.Flags & BucketAuthNoToken) != 0 {
		return
	}


	calc_auth, err := auth.GenerateSignature(acl.Token, r.Method, r.URL, r.Header)
	if err != nil {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("auth: header: '%v': hmac generation failed: %s",
				r.Header[auth.AuthHeaderStr], user))
		return
	}

	if recv_auth != calc_auth {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("auth: header: '%v': user: %s, hmac mismatch: recv: '%s', calc: '%s'",
				r.Header[auth.AuthHeaderStr], user, recv_auth, calc_auth))
		return
	}

	return
}

func (bucket *Bucket) lookup_serialize(write bool, ch <-chan elliptics.Lookuper) (map[string]interface{}, error) {
	var info []interface{}
	var egroups, sgroups []uint32

	egroups = make([]uint32, 0)
	sgroups = make([]uint32, 0)

	var err error
	for l := range ch {
		ret := make(map[string]interface{})
		if l.Error() != nil {
			egroups = append(egroups, l.Cmd().ID.Group)

			ret["error"] = fmt.Sprintf("%v", l.Error())
			err = l.Error()
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

	reply := make(map[string]interface{})
	reply["info"] = info
	reply["success-groups"] = sgroups
	reply["error-groups"] = egroups

	if len(sgroups) != 0 {
		err = nil
	}

	return reply, err
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

	for _, b := range bctl.Bucket {
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

func ReadBucket(ell *etransport.Elliptics, name string) (bucket *Bucket, err error) {
	ms, err := ell.MetadataSession()
	if err != nil {
		log.Printf("%s: could not create metadata session: %v", name, err)
		return
	}

	ms.SetNamespace(BucketNamespace)

	b := &Bucket {
		Name:		name,
		Group:		make(map[uint32]*elliptics.StatGroup),

		Time:		time.Now(),
	}

	for rd := range ms.ReadData(name, 0, 0) {
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

		err = b.Meta.ExtractMsgpack(out)
		if err != nil {
			log.Printf("%s: unsupported msgpack data: %v", name, err)
			return
		}

		log.Printf("%s: groups: %v, acl: %v\n", b.Name, b.Meta.Groups, b.Meta.Acl)
		bucket = b
		return
	}

	bucket = nil
	err = errors.NewKeyError(name, http.StatusNotFound,
		"could not read bucket data: ReadData() returned nothing")
	return
}

func WriteBucket(ell *etransport.Elliptics, meta *BucketMsgpack) (bucket *Bucket, err error) {
	ms, err := ell.MetadataSession()
	if err != nil {
		log.Printf("%s: could not create metadata session: %v", meta.Name, err)
		return
	}

	ms.SetNamespace(BucketNamespace)

	out, err := meta.PackMsgpack()
	if err != nil {
		log.Printf("%s: could not pack bucket: %v", meta.Name, err)
		return
	}

	data, err := msgpack.Marshal(&out)
	if err != nil {
		log.Printf("%s: could not parse bucket metadata: %v", meta.Name, err)
		return
	}

	for wr := range ms.WriteData(meta.Name, bytes.NewReader(data), 0, 0) {
		if wr.Error() != nil {
			err = wr.Error()

			log.Printf("%s: could not write bucket metadata: %v", meta.Name, err)
			return
		}

		bucket = &Bucket {
			Meta:		*meta,
			Name:		meta.Name,
			Group:		make(map[uint32]*elliptics.StatGroup),

			Time:		time.Now(),
		}

		return
	}

	err = errors.NewKeyError(meta.Name, http.StatusNotFound,
		"could not write bucket metadata: WriteData() returned nothing")
	return
}

func WriteBucketJson(ell *etransport.Elliptics, name string, data []byte) (bucket *Bucket, err error) {
	meta := BucketMsgpack {
		Version:	1,
		Name:		name,
		Acl:		make(map[string]BucketACL),
	}

	// this can not create ACL map from array
	err = json.Unmarshal(data, &meta)
	if err != nil {
		err = fmt.Errorf("could not parse data: %v", err)
		return
	}

	var iface interface{}
	err = json.Unmarshal(data, &iface)
	if err != nil {
		err = fmt.Errorf("could not parse data: %v", err)
		return
	}

	imap := iface.(map[string]interface{})

	log.Printf("acl: %v\n", imap["acl"])

	for _, i := range imap["acl"].([]interface{}) {
		acl := BucketACL {
			Version: 1,
		}

		x := i.(map[string]interface{})
		if v, ok := x["user"].(string); ok {
			acl.User = v
		} else {
			err = fmt.Errorf("acl: could not find user")
			return
		}
		if v, ok := x["token"].(string); ok {
			acl.Token = v
		} else {
			err = fmt.Errorf("acl: could not find token")
			return
		}
		if v, ok := x["flags"].(float64); ok {
			acl.Flags = uint64(v)
		} else {
			err = fmt.Errorf("acl: could not find flags")
			return
		}

		meta.Acl[acl.User] = acl
	}

	return WriteBucket(ell, &meta)
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
