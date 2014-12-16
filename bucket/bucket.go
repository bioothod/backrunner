package bucket

import (
	"bytes"
	"encoding/json"
	"encoding/hex"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"github.com/bioothod/elliptics-go/elliptics"
	"github.com/vmihailenco/msgpack"
	"fmt"
	"log"
	"net/http"
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

func (meta *BucketMsgpack) String() string {
	return fmt.Sprintf("%s: groups: %v, acl: %v, flags: 0x%x, max-size: %d, max-key-num: %d",
		meta.Name, meta.Groups, meta.Acl, meta.Flags, meta.MaxSize, meta.MaxKeyNum)
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

func cast_to_uint64(val interface{}) (uint64, bool) {
	x, ok := val.(uint64)
	if ok {
		return x, ok
	}

	x1, ok := val.(int64)
	if ok {
		return uint64(x1), ok
	}

	return 0, false
}

func (meta *BucketMsgpack) ExtractMsgpack(out []interface{}) (err error) {
	var ok bool

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
		if v, ok := cast_to_uint64(x[0]); ok {
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
		if v, ok := cast_to_uint64(x[3]); ok {
			acl.Flags = uint64(v)
		} else {
			return fmt.Errorf("acl: could not find flags")
		}

		meta.Acl[acl.User] = acl
	}

	for _, x := range out[3].([]interface{}) {
		g, ok := cast_to_uint64(x)
		if !ok {
			return fmt.Errorf("could not cast group '%v'", x)
		}

		meta.Groups = append(meta.Groups, uint32(g))
	}
	meta.Flags, ok = cast_to_uint64(out[4])
	if !ok {
		return fmt.Errorf("could not cast flags '%v'", out[4])
	}

	meta.MaxSize, ok = cast_to_uint64(out[5])
	if !ok {
		return fmt.Errorf("could not cast max-size '%v'", out[5])
	}

	meta.MaxKeyNum, ok = cast_to_uint64(out[6])
	if !ok {
		return fmt.Errorf("could not cast max-key-num '%v'", out[6])
	}

	for i := range meta.reserved {
		meta.reserved[i], _ = cast_to_uint64(out[7 + i])
	}

	return nil
}

type Bucket struct {
	Name		string
	Group		map[uint32]*elliptics.StatGroup

	Meta		BucketMsgpack
}

func NewBucket(name string) *Bucket {
	return &Bucket {
		Name:		name,
		Group:		make(map[uint32]*elliptics.StatGroup),
	}
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

func (bucket *Bucket) lookup_serialize(write bool, ch <-chan elliptics.Lookuper) (*reply.LookupResult, error) {
	r := &reply.LookupResult {
		Servers:		make([]*reply.LookupServerResult, 0, 2),
		SuccessGroups:		make([]uint32, 0, 2),
		ErrorGroups:		make([]uint32, 0, 2),
	}

	var err error
	for l := range ch {
		ret := &reply.LookupServerResult {
			Group: l.Cmd().ID.Group,
			Backend: l.Cmd().Backend,
			Addr: l.Addr(),
		}
		if l.Error() != nil {
			r.ErrorGroups = append(r.ErrorGroups, l.Cmd().ID.Group)
			err = l.Error()
			dnet_error := elliptics.DnetErrorFromError(err)
			if dnet_error != nil {
				ret.Error = dnet_error
			} else {
				ret.Error = &elliptics.DnetError {
					Code:	-22,
					Flags:	l.Cmd().Flags,
					Message: err.Error(),
				}
			}
		} else {
			r.SuccessGroups = append(r.SuccessGroups, l.Cmd().ID.Group)

			ret.IDString = hex.EncodeToString(l.Cmd().ID.ID)
			ret.CsumString = hex.EncodeToString(l.Info().Csum)
			ret.Filename = l.Path()
			ret.Size = l.Info().Size
			ret.Offset = l.Info().Offset
			ret.MtimeString = l.Info().Mtime.String()
			ret.ServerString = l.StorageAddr().String()

			ret.Server = l.StorageAddr()
			ret.Info = l.Info()
		}


		r.Servers = append(r.Servers, ret)
	}

	if len(r.SuccessGroups) != 0 {
		err = nil
	}

	return r, err
}

func ReadBucket(ell *etransport.Elliptics, name string) (bucket *Bucket, err error) {
	ms, err := ell.MetadataSession()
	if err != nil {
		log.Printf("read-bucket: %s: could not create metadata session: %v", name, err)
		return
	}

	ms.SetNamespace(BucketNamespace)

	b := NewBucket(name)

	for rd := range ms.ReadData(name, 0, 0) {
		if rd.Error() != nil {
			err = rd.Error()

			log.Printf("read-bucket: %s: could not read bucket metadata: %v", name, err)
			return
		}

		var out []interface{}
		err = msgpack.Unmarshal([]byte(rd.Data()), &out)
		if err != nil {
			log.Printf("read-bucket: %s: could not parse bucket metadata: %v", name, err)
			return
		}

		err = b.Meta.ExtractMsgpack(out)
		if err != nil {
			log.Printf("read-bucket: %s: unsupported msgpack data: %v", name, err)
			return
		}

		bucket = b
		return
	}

	bucket = nil
	err = errors.NewKeyError(name, http.StatusNotFound,
		"read-bucket: could not read bucket data: ReadData() returned nothing")
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

		bucket = NewBucket(meta.Name)
		bucket.Meta = *meta

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
			Version: 2,
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
