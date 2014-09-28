package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

var (
	proxy bproxy
	IdleTimeout		= 150 * time.Second
)

type bproxy struct {
	host		string
	bctl		*bucket.BucketCtl
	ell		*etransport.Elliptics
}

type Reply struct {
	status		int
	err		error
}

func GoodReply() Reply {
	return Reply {
		err: nil,
		status: http.StatusOK,
	}
}

func ping_handler(w http.ResponseWriter, r *http.Request, strings ...string) Reply {
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

	return GoodReply()
}

func (p *bproxy) local_url(key, bucket, operation string) string {
	return fmt.Sprintf("http://%s/%s/%s/%s", p.host, operation, bucket, key)
}

func (p *bproxy) send_upload_reply(w http.ResponseWriter, req *http.Request,
		bucket *bucket.Bucket, key string, resp map[string]interface{}) Reply {
	reply := reply.Upload {
		Bucket: bucket.Name,
		Reply:  &resp,
		Primary: reply.Entry {
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

		return Reply {
			err: err,
			status: http.StatusBadRequest,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)

	return GoodReply()
}

func nobucket_upload_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	key := strings[0]

	resp, bucket, err := proxy.bctl.Upload(key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	return proxy.send_upload_reply(w, req, bucket, key, resp)
}

func bucket_upload_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	bucket := strings[0]
	key := strings[1]

	resp, b, err := proxy.bctl.BucketUpload(bucket, key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	return proxy.send_upload_reply(w, req, b, key, resp)
}

func get_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	bucket := strings[0]
	key := strings[1]

	err := proxy.bctl.Stream(bucket, key, w, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	return GoodReply()
}

func lookup_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	bucket := strings[0]
	key := strings[1]

	reply, err := proxy.bctl.Lookup(bucket, key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: lookup: json marshal failed: %q\n", req.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return Reply {
			err: err,
			status: http.StatusBadRequest,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)

	return GoodReply()
}

func delete_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	bucket := strings[0]
	key := strings[1]

	err := proxy.bctl.Delete(bucket, key, req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	w.WriteHeader(http.StatusOK)

	return GoodReply()
}

func bulk_delete_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	bucket := strings[0]

	var err error
	var v map[string]interface{} = make(map[string]interface{})
        if err = json.NewDecoder(req.Body).Decode(&v); err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("bulk_delete: could not parse input json: %v", err))
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
        }

	kv, ok := v["keys"]
	if !ok {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("bulk_delete: there is no 'keys' array"))
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	var keys []string = make([]string, 0)

	for _, v := range kv.([]interface{}) {
		keys = append(keys, v.(string))
	}

	if len(keys) == 0 {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("bulk_delete: 'keys' array is empty"))
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply, err := proxy.bctl.BulkDelete(bucket, keys, req)
	log.Printf("reply: %v, err: %v\n", reply, err)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: bulk_delete: json marshal failed: %q\n", req.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return Reply {
			err: err,
			status: http.StatusBadRequest,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)

	return GoodReply()
}

func stat_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	reply, err := proxy.bctl.Stat(req)
	if err != nil {
		http.Error(w, errors.ErrorData(err), errors.ErrorStatus(err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: stat: json marshal failed: %q\n", req.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return Reply {
			err: err,
			status: http.StatusBadRequest,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)

	return GoodReply()
}

type handler struct {
	params			int // minimal number of path components after /handler/ needed to run this handler
	function		func(w http.ResponseWriter, req *http.Request, v...string) Reply
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
	"/delete/" : {
		params: 2,
		function: delete_handler,
	},
	"/bulk_delete/" : {
		params: 1,
		function: bulk_delete_handler,
	},
	"/ping/" : {
		params: 0,
		function: ping_handler,
	},
	"/stat/" : {
		params: 0,
		function: stat_handler,
	},
}

func generic_handler(w http.ResponseWriter, req *http.Request) {
	// join together sequential // in the URL path

	reply := Reply {
		status: http.StatusBadRequest,
		err: errors.NewKeyError(req.URL.String(), http.StatusBadRequest, "there is no registered handler for this path"),
	}
	need_flush := true

	path, err := url.QueryUnescape(req.URL.Path)
	if err != nil {
		path = req.URL.Path
		reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("could not unescape URL: %v", err))
	} else {
		for k, v := range proxy_handlers {
			if (strings.HasPrefix(path, k)) {
				prefix := path[len(k):]
				tmp := strings.SplitN(prefix, "/", v.params)

				if len(tmp) >= v.params {
					reply = v.function(w, req, tmp...)
					need_flush = false
					break
				}
			}
		}
	}

	msg := "OK"
	if reply.err != nil {
		msg = reply.err.Error()
	}

	log.Printf("access_log: path: '%s', encoded-uri: '%s', status: %d, err: '%v'\n", path, req.URL.RequestURI(), reply.status, msg)

	if need_flush {
		http.Error(w, reply.err.Error(), http.StatusBadRequest)
	}
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
