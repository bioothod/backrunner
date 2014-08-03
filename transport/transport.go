package transport

import (
	"github.com/bioothod/backrunner/bucket"
	"net/http"
	"time"
)

const DEFAULT_IDLE_TIMEOUT = 5 * time.Second

type Request struct {
	Bctl	*bucket.BucketCtl
	Bucket	*bucket.Bucket
	Http	*http.Request

	User	string
	Key	string
}

type Response struct {
	Status	int
	Data	[]byte
	Json	interface{}
}

type Transport interface {
	Upload(req *Request) (resp *Response, err error)
}
