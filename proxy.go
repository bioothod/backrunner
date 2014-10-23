package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/config"
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
)

type bproxy struct {
	bctl		*bucket.BucketCtl
	ell		*etransport.Elliptics
	conf		*config.ProxyConfig
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
	return fmt.Sprintf("http://%s/%s/%s/%s", p.conf.Proxy.Address, operation, bucket, key)
}

func (p *bproxy) send_upload_reply(w http.ResponseWriter, req *http.Request,
		bucket *bucket.Bucket, key string, resp *reply.LookupResult) Reply {
	reply := reply.Upload {
		Bucket: bucket.Name,
		Reply:  resp,
		Primary: reply.Entry {
			Key:	key,
			Get:    "GET " + proxy.local_url(key, bucket.Name, "get"),
			Update: "POST " + proxy.local_url(key, bucket.Name, "upload"),
			Delete: "POST " + proxy.local_url(key, bucket.Name, "delete"),
		},
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("upload: json marshal failed: %q", err))

		return Reply {
			err: err,
			status: http.StatusServiceUnavailable,
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
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("lookup: json marshal failed: %q", err))
		return Reply {
			err: err,
			status: http.StatusServiceUnavailable,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)

	return GoodReply()
}

func redirect_handler(w http.ResponseWriter, req *http.Request, string_keys ...string) Reply {
	if proxy.conf.Proxy.RedirectPort == 0 || proxy.conf.Proxy.RedirectPort >= 65536 {
		err := errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
				fmt.Sprintf("redirect is not allowed because of invalid redirect port %d",
					proxy.conf.Proxy.RedirectPort))

		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	bucket := string_keys[0]
	key := string_keys[1]

	reply, err := proxy.bctl.Lookup(bucket, key, req)
	if err != nil {
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	srv := reply.Servers[rand.Intn(len(reply.Servers))]
	scheme := "http"
	if req.URL.Scheme != "" {
		scheme = req.URL.Scheme
	}

	if len(srv.Filename) == 0 {
		err := errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("lookup returned invalid filename: %s", srv.Filename))

		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	filename := srv.Filename

	if len(proxy.conf.Proxy.RedirectRoot) != 0{
		if strings.HasPrefix(filename, proxy.conf.Proxy.RedirectRoot) {
			filename = filename[len(proxy.conf.Proxy.RedirectRoot):]
		}
	}

	slash := "/"
	if filename[0] == '/' {
		slash = ""
	}

	timestamp := time.Now().Unix()
	url_str := fmt.Sprintf("%s://%s:%d%s%s:%d:%d",
		scheme, srv.Server.HostString(), proxy.conf.Proxy.RedirectPort,
		slash, filename, srv.Offset, srv.Size)

	u, err := url.Parse(url_str)
	if err != nil {
		err := errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
				fmt.Sprintf("could not parse generated redirect url '%s'", url_str))

		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	req.URL = u

	w.Header().Set("X-Ell-Mtime", fmt.Sprintf("%d", srv.Info.Mtime.Unix()))
	w.Header().Set("X-Ell-Signtime", fmt.Sprintf("%d", timestamp))
	w.Header().Set("X-Ell-SignatureTimeout", fmt.Sprintf("%d", proxy.conf.Proxy.RedirectSignatureTimeout))
	w.Header().Set("X-Ell-Offset", fmt.Sprintf("%d", srv.Offset))
	w.Header().Set("X-Ell-Size", fmt.Sprintf("%d", srv.Size))
	w.Header().Set("X-Ell-File", filename)

	signature, err := auth.GenerateSignature(proxy.conf.Proxy.RedirectToken, "GET", req.URL, w.Header())
	if err != nil {
		err := errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("could not generate signature for redirect url '%s': %v", url_str, err))

		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	w.Header().Set(auth.AuthHeaderStr, signature)

	http.Redirect(w, req, url_str, http.StatusFound)

	return GoodReply()
}


func delete_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	bucket := strings[0]
	key := strings[1]

	err := proxy.bctl.Delete(bucket, key, req)
	if err != nil {
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
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
        }

	kv, ok := v["keys"]
	if !ok {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("bulk_delete: there is no 'keys' array"))
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
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply, err := proxy.bctl.BulkDelete(bucket, keys, req)
	log.Printf("reply: %v, err: %v\n", reply, err)
	if err != nil {
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: bulk_delete: json marshal failed: %q\n", req.URL, err)
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
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("stat: json marshal failed: %q", err))
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
	methods			[]string // GET, POST and so on - methods which are allowed to be used with this handler
	function		func(w http.ResponseWriter, req *http.Request, v...string) Reply
}

var proxy_handlers = map[string]handler {
	"/nobucket_upload/" : {
		params:	1,
		methods: []string{"POST", "PUT"},
		function: nobucket_upload_handler,
	},
	"/upload/" : {
		params: 2,
		methods: []string{"POST", "PUT"},
		function: bucket_upload_handler,
	},
	"/get/" : {
		params: 2,
		methods: []string{"GET"},
		function: get_handler,
	},
	"/lookup/" : {
		params: 2,
		methods: []string{"GET"},
		function: lookup_handler,
	},
	"/redirect/" : {
		params: 2,
		methods: []string{"GET"},
		function: redirect_handler,
	},
	"/delete/" : {
		params: 2,
		methods: []string{"POST", "PUT"},
		function: delete_handler,
	},
	"/bulk_delete/" : {
		params: 1,
		methods: []string{"POST", "PUT"},
		function: bulk_delete_handler,
	},
	"/ping/" : {
		params: 0,
		methods: []string{"GET"},
		function: ping_handler,
	},
	"/stat/" : {
		params: 0,
		methods: []string{"GET"},
		function: stat_handler,
	},
}

func generic_handler(w http.ResponseWriter, req *http.Request) {
	// join together sequential // in the URL path

	start := time.Now()

	reply := Reply {
		status: http.StatusBadRequest,
		err: errors.NewKeyError(req.URL.String(), http.StatusBadRequest, "there is no registered handler for this path"),
	}

	path, err := url.QueryUnescape(req.URL.Path)
	if err != nil {
		path = req.URL.Path
		reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("could not unescape URL: %v", err))
	} else {
		for k, v := range proxy_handlers {
			if (strings.HasPrefix(path, k)) {
				method_matched := false
				for _, method := range v.methods {
					if method == req.Method {
						method_matched = true
						break
					}
				}

				if method_matched {
					prefix := path[len(k):]
					tmp := strings.SplitN(prefix, "/", v.params)

					if len(tmp) >= v.params {
						reply = v.function(w, req, tmp...)
						break
					}
				}
			}
		}
	}

	msg := "OK"
	if reply.err != nil {
		msg = reply.err.Error()
	}

	log.Printf("access_log: method: '%s', path: '%s', encoded-uri: '%s', status: %d, duration: %.3f ms, err: '%v'\n",
		req.Method, path, req.URL.RequestURI(), reply.status, float64(time.Since(start).Nanoseconds()) / 1000000.0, msg)

	if reply.err != nil {
		http.Error(w, reply.err.Error(), reply.status)
	}
}

func (proxy *bproxy) getTimeoutServer(addr string, handler http.Handler) *http.Server {
	//keeps people who are slow or are sending keep-alives from eating all our sockets
	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  time.Duration(proxy.conf.Proxy.IdleTimeout) * time.Second,
		WriteTimeout:  time.Duration(proxy.conf.Proxy.IdleTimeout) * time.Second,
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

	buckets := flag.String("buckets", "", "buckets file (file format: new-line separated list of bucket names)")
	config_file := flag.String("config", "", "Transport config file")
	flag.Parse()

	if *buckets == "" {
		log.Fatal("there is no buckets file")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	var err error

	proxy.conf = &config.ProxyConfig {}
	err = proxy.conf.Load(*config_file)
	if err != nil {
		log.Fatalf("Could not load config %s: %q", config_file, err)
	}

	if len(proxy.conf.Proxy.Address) == 0 {
		log.Fatalf("'address' must be specified in proxy config '%s'\n", *config_file)
	}

	if proxy.conf.Proxy.RedirectPort == 0 || proxy.conf.Proxy.RedirectPort >= 65536 {
		log.Printf("redirect is not allowed because of invalid redirect port %d",
			proxy.conf.Proxy.RedirectPort)
	}

	proxy.ell, err = etransport.NewEllipticsTransport(proxy.conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	rand.Seed(9)

	proxy.bctl, err = bucket.NewBucketCtl(proxy.ell, *buckets, *config_file)
	if err != nil {
		log.Fatalf("Could not process buckets file '%s': %v", *buckets, err)
	}

	server := proxy.getTimeoutServer(proxy.conf.Proxy.Address, http.HandlerFunc(generic_handler))

	log.Fatal(server.ListenAndServe())
}
