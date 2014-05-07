package main

import (
	"encoding/json"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"os"
)

var (
	DeleteIndex     string   = "delete"
	BucketNamespace string   = "bucket"
	Buckets         []string = []string{"bucket:12.21", "bucket:22.31", "bucket:32.11"}
)

type Delentry struct {
	time int64
	key  string
}

func (del *Delentry) pack() ([]byte, error) {
	in := map[string]interface{}{
		"time": del.time,
		"key":  del.key,
	}
	return msgpack.Marshal(in)
}

func (del *Delentry) unpack(data []byte) (err error) {
	var out map[string]interface{}
	err = msgpack.Unmarshal(data, &out)
	if err != nil {
		return err
	}

	del.time = out["time"].(int64)
	del.key = out["key"].(string)

	return nil
}

type BucketACL struct {
	version int32
	user string
	token string
	flags uint64
}
type acl_json struct {
	User string `json:"user"`
	Token string `json:"token"`
	Flags uint64 `json:"flags"`
}

func BucketACL_Extract_JSON_File(path string) (acl map[string]BucketACL, err error) {
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

	acl = make(map[string]BucketACL)
	for _, a := range jacl {
		var e BucketACL

		e.user = a.User
		e.token = a.Token
		e.flags = a.Flags
		e.version = 1

		acl[e.user] = e
	}

	return
}

type BucketMeta struct {
	version int32
	bucket string
	acl map[string]BucketACL
	groups []int32
	flags uint64
	max_size uint64
	max_key_num uint64
	reserved [3]uint64
}

type ExtractError struct {
	reason string
	out []interface{}
}

func (err *ExtractError) Error() string {
	return fmt.Sprintf("%s: %v", err.reason, err.out)
}

func (meta *BucketMeta) ExtractMsgpack(out []interface{}) (err error) {
	if len(out) < 8 {
		return &ExtractError{
			reason: fmt.Sprintf("array length: %d, must be at least 8", len(out)),
			out: out,
		}
	}
	meta.version = int32(out[0].(int64))
	if meta.version != 1 {
		return &ExtractError{
			reason: fmt.Sprintf("unsupported metadata version %d", meta.version),
			out: out,
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

