package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

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

type Bucket struct {
	Name	string
	Rate	float64
}

func NewBucket(name string) Bucket {
	fmt.Printf("bucket: %s\n", name)
	return Bucket {
		Name: name,
		Rate: 0.0,
	}
}

type BucketCtl struct {
	bucket		[]Bucket
	acl		map[string]BucketACL
}

var (
	BucketNamespace string   = "bucket"
)

func (bucket *BucketCtl) open_acl(path string) (err error) {
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

	bucket.acl = make(map[string]BucketACL)
	for _, a := range jacl {
		var e BucketACL

		e.user = a.User
		e.token = a.Token
		e.flags = a.Flags
		e.version = 1

		bucket.acl[e.user] = e
	}

	return
}

func NewBucketCtl(bucket_path, acl_path string) (bucket BucketCtl, err error) {
	bucket = BucketCtl{
		bucket: make([]Bucket, 0, 10),
		acl: make(map[string]BucketACL),
	}

	data, err := ioutil.ReadFile(bucket_path)
	if err != nil {
		return
	}

	for _, name := range strings.Split(string(data), "\n") {
		if len(name) > 0 {
			bucket.bucket = append(bucket.bucket, NewBucket(name))
		}
	}

	if len(bucket.bucket) == 0 {
		log.Fatal("No buckets found in bucket file")
	}

	err = bucket.open_acl(acl_path)
	if err != nil {
		log.Fatal("Failed to process ACL file", err)
	}

	return bucket, nil
}

type ExtractError struct {
	reason string
	out []interface{}
}

func (err *ExtractError) Error() string {
	return fmt.Sprintf("%s: %v", err.reason, err.out)
}

type BucketMsgpack struct {
	version int32
	bucket string
	acl map[string]BucketACL
	groups []int32
	flags uint64
	max_size uint64
	max_key_num uint64
	reserved [3]uint64
}

func (meta *BucketMsgpack) ExtractMsgpack(out []interface{}) (err error) {
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
