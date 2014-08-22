package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/etransport"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
	"time"
)

var (
	proxy bproxy
	IdleTimeout		= 50 * time.Second
)

type bproxy struct {
	host		string
	bctl		*bucket.BucketCtl
	ell		*etransport.Elliptics
}

func ping_handler(w http.ResponseWriter, r *http.Request, strings ...string) {
	message := "Ping OK"

	buckets := make([]interface{}, 0)

	for i := range proxy.bctl.Bucket {
		b := &proxy.bctl.Bucket[i]
		buckets = append(buckets, b)
	}

	j, err := json.Marshal(buckets)
	if err == nil {
		message = string(j)
	} else {
		message = fmt.Sprintf("marshaling error: %v", err)
	}
	http.Error(w, message, http.StatusOK)
}

func (p *bproxy) local_url(key, bucket, operation string) string {
	return fmt.Sprintf("http://%s/%s/%s/%s", p.host, operation, bucket, key)
}

func (p *bproxy) send_upload_reply(w http.ResponseWriter, req *http.Request, bucket *bucket.Bucket, key string, resp map[string]interface{}) {
	type ent_reply struct {
		Get    string `json:"get"`
		Update string `json:"update"`
		Delete string `json:"delete"`
		Key    string `json:"key"`
	}
	type upload_reply struct {
		Bucket  string       `json:"bucket"`
		Primary ent_reply    `json:"primary"`
		Reply   *map[string]interface{} `json:"reply"`
	}

	reply := upload_reply {
		Bucket: bucket.Name,
		Reply:  &resp,
		Primary: ent_reply{
			Key:	key,
			Get:    "GET " + proxy.local_url(key, bucket.Name, "get"),
			Update: "POST " + proxy.local_url(key, bucket.Name, "upload"),
			Delete: "POST " + proxy.local_url(key, bucket.Name, "delete"),
		},
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: upload: json marshal failed: %q\n", req.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)
}

func nobucket_upload_handler(w http.ResponseWriter, req *http.Request, strings ...string) {
	key := strings[0]

	resp, bucket, err := proxy.bctl.Upload(key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return
	}

	proxy.send_upload_reply(w, req, bucket, key, resp)
	return
}

func bucket_upload_handler(w http.ResponseWriter, req *http.Request, strings ...string) {
	bucket := strings[0]
	key := strings[1]

	resp, b, err := proxy.bctl.BucketUpload(bucket, key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return
	}

	proxy.send_upload_reply(w, req, b, key, resp)
	return
}

func get_handler(w http.ResponseWriter, req *http.Request, strings ...string) {
	bucket := strings[0]
	key := strings[1]

	err := proxy.bctl.Stream(bucket, key, w, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return
	}
	return
}

func lookup_handler(w http.ResponseWriter, req *http.Request, strings ...string) {
	bucket := strings[0]
	key := strings[1]

	reply, err := proxy.bctl.Lookup(bucket, key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: lookup: json marshal failed: %q\n", req.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)
}

type handler struct {
	params			int
	function		func(w http.ResponseWriter, req *http.Request, v...string)
}

var proxy_handlers = map[string]handler {
	"/nobucket_upload/" : {
		params:	1,
		function: nobucket_upload_handler,
	},
	"/upload/" : {
		params: 2,
		function: bucket_upload_handler,
	},
	"/get/" : {
		params: 2,
		function: get_handler,
	},
	"/lookup/" : {
		params: 2,
		function: lookup_handler,
	},
	"/ping/" : {
		params: 0,
		function: ping_handler,
	},
}

func generic_handler(w http.ResponseWriter, req *http.Request) {
	for k, v := range proxy_handlers {
		if (strings.HasPrefix(req.URL.Path, k)) {
			path := req.URL.Path[len(k):]
			tmp := strings.SplitN(path, "/", v.params)

			if len(tmp) >= v.params {
				v.function(w, req, tmp...)
				return
			}
		}
	}

	err := errors.NewKeyError(req.URL.String(), http.StatusBadRequest, "there is no registered handler for this path")
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func getTimeoutServer(addr string, handler http.Handler) *http.Server {
	//keeps people who are slow or are sending keep-alives from eating all our sockets
	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  IdleTimeout,
		WriteTimeout: IdleTimeout,
	}
}

type stringslice []string

func (str *stringslice) String() string {
	return fmt.Sprintf("%d", *str)
}

func (str *stringslice) Set(value string) error {
	*str = append(*str, value)
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	listen := flag.String("listen", "0.0.0.0:9090", "listen and serve address")
	buckets := flag.String("buckets", "", "buckets file (file format: new-line separated list of bucket names)")
	config := flag.String("config", "", "Transport config file")
	flag.Parse()

	if *buckets == "" {
		log.Fatal("there is no buckets file")
	}

	if *config == "" {
		log.Fatal("You must specify config file")
	}
	var err error
	proxy.ell, err = etransport.NewEllipticsTransport(*config)

	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	rand.Seed(9)

	proxy.bctl, err = bucket.NewBucketCtl(proxy.ell, *buckets)
	if err != nil {
		log.Fatalf("Could not process buckets file '%s': %v", *buckets, err)
	}

	proxy.host = "localhost"


	server := getTimeoutServer(*listen, http.HandlerFunc(generic_handler))

	log.Fatal(server.ListenAndServe())
}
