package main

import (
	"github.com/vmihailenco/msgpack"
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
