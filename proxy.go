package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/estimator"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const last_errors_length int = 128

var (
	proxy bproxy
)

type ErrorInfo struct {
	Tsec		int64
	Tnsec		int
	Time		string
	Method		string
	RemoteAddr	string
	URL		string
	Error		string
	Status		int
}

type bproxy struct {
	bctl		*bucket.BucketCtl
	ell		*etransport.Elliptics

	error_index	uint64
	last_errors	[]ErrorInfo
}

type Reply struct {
	status		int
	length		uint64
	err		error
}

func GoodReply() Reply {
	return Reply {
		err: nil,
		status: http.StatusOK,
	}
}

func GoodReplyLength(length uint64) Reply {
	return Reply {
		err: nil,
		status: http.StatusOK,
		length: length,
	}
}

func (p *bproxy) add_error(method, addr, url string, status int, err string) {
	idx := (atomic.AddUint64(&p.error_index, 1) - 1) % uint64(len(p.last_errors))
	t := time.Now()
	p.last_errors[idx] = ErrorInfo {
		Tsec:		t.Unix(),
		Tnsec:		t.Nanosecond(),
		Time:		t.String(),

		Method:		method,
		RemoteAddr:	addr,
		URL:		url,
		Error:		err,
		Status:		status,
	}
}

func (p *bproxy) send_upload_reply(w http.ResponseWriter, req *http.Request,
		bucket *bucket.Bucket, key string, resp *reply.LookupResult) Reply {
	reply := reply.Upload {
		Bucket: bucket.Name,
		Key: key,
		Reply:  resp,
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
	if proxy.bctl.Conf.Proxy.RedirectPort == 0 || proxy.bctl.Conf.Proxy.RedirectPort >= 65536 {
		err := errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
				fmt.Sprintf("redirect is not allowed because of invalid redirect port %d",
					proxy.bctl.Conf.Proxy.RedirectPort))

		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	bname := string_keys[0]
	key := string_keys[1]

	reply, err := proxy.bctl.Lookup(bname, key, req)
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

	if len(proxy.bctl.Conf.Proxy.RedirectRoot) != 0{
		if strings.HasPrefix(filename, proxy.bctl.Conf.Proxy.RedirectRoot) {
			filename = filename[len(proxy.bctl.Conf.Proxy.RedirectRoot):]
		}
	}

	slash := "/"
	if filename[0] == '/' {
		slash = ""
	}

	offset, size, err := bucket.URIOffsetSize(req)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("redirect: %v", err))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	if offset >= srv.Size {
		err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
			fmt.Sprintf("redirect: offset is beyond size of the object: offset: %d, size: %d",
				offset, srv.Size))
		return Reply {
			err: err,
			status: errors.ErrorStatus(err),
		}
	}

	if size == 0 || offset + size >= srv.Size {
		size = srv.Size - offset
	}

	timestamp := time.Now().Unix()
	url_str := fmt.Sprintf("%s://%s:%d%s%s:%d:%d",
		scheme, srv.Server.HostString(), proxy.bctl.Conf.Proxy.RedirectPort,
		slash, filename, srv.Offset + offset, size)

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
	w.Header().Set("X-Ell-Signature-Timeout", fmt.Sprintf("%d", proxy.bctl.Conf.Proxy.RedirectSignatureTimeout))
	w.Header().Set("X-Ell-File-Offset", fmt.Sprintf("%d", srv.Offset))
	w.Header().Set("X-Ell-Total-Size", fmt.Sprintf("%d", srv.Size))
	w.Header().Set("X-Ell-File", filename)

	signature, err := auth.GenerateSignature(proxy.bctl.Conf.Proxy.RedirectToken, "GET", req.URL, w.Header())
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

func common_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	if len(proxy.bctl.Conf.Proxy.Root) == 0 {
		err := errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("common: root option is not configured, reading files is being denied"))
		return Reply {
			err: err,
			status: http.StatusServiceUnavailable,
		}
	}

	if len(strings) == 0 {
		w.WriteHeader(http.StatusOK)
		return GoodReply()
	}

	object := path.Clean(strings[0])
	if object == bucket.ProfilePath || object == "." {
		err := errors.NewKeyError(req.URL.String(), http.StatusNotFound,
			fmt.Sprintf("common: could not read file '%s'", object))
		return Reply {
			err: err,
			status: http.StatusNotFound,
		}
	}

	key := proxy.bctl.Conf.Proxy.Root + "/" + object

	data, err := ioutil.ReadFile(key)
	if err != nil {
		log.Printf("common: url: %s, object: '%s', error: %s\n", req.URL.String(), object, err)

		err = errors.NewKeyError(req.URL.String(), http.StatusNotFound,
			fmt.Sprintf("common: could not read file '%s'", object))
		return Reply {
			err: err,
			status: http.StatusNotFound,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(data)

	return GoodReplyLength(uint64(len(data)))
}

func profile_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	proxy.bctl.DumpProfile(w)
	return GoodReply()
}

func exit_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	proxy.bctl.DumpProfileFile(true)
	proxy.bctl.DumpProfile(w)
	os.Exit(-1)
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


// this uglymoron is needed to prevent Golang initialization loop logic from exploding
var estimator_scan_handlers map[string]*handler

type proxy_stat_reply struct {
	BucketCtlStat	*bucket.BucketCtlStat
	Handlers	map[string]*handler	`json:"handlers"`
	Errors		[]ErrorInfo		`json:"errors"`
}

func proxy_stat_handler(w http.ResponseWriter, req *http.Request, strings ...string) Reply {
	start_idx := proxy.error_index
	l := uint64(len(proxy.last_errors))
	if start_idx < uint64(len(proxy.last_errors)) {
		l = start_idx
	}

	res := proxy_stat_reply {
		BucketCtlStat:	proxy.bctl.NewBucketCtlStat(),
		Handlers:	estimator_scan_handlers,
		Errors:		make([]ErrorInfo, l),
	}

	if start_idx <= uint64(len(proxy.last_errors)) {
		copy(res.Errors, proxy.last_errors)
	} else {
		var i uint64
		for i = 0; i < uint64(len(proxy.last_errors)); i++ {
			idx := (i + start_idx + 1) % uint64(len(proxy.last_errors))
			res.Errors[i] = proxy.last_errors[idx]
		}
	}

	reply_json, err := json.Marshal(&res)
	if err != nil {
		err = errors.NewKeyError(req.URL.String(), http.StatusServiceUnavailable,
			fmt.Sprintf("stat: json marshal failed: %q", err))
		return Reply {
			err: err,
			status: http.StatusServiceUnavailable,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)

	return GoodReply()
}

type handler struct {
	// minimal number of path components after /handler/ needed to run this handler
	Params			int				`json:"-"`

	// GET, POST and so on - Methods which are allowed to be used with this handler
	Methods			[]string			`json:"-"`

	// handler Function
	Function		func(w http.ResponseWriter, req *http.Request, v...string) Reply		`json:"-"`

	Estimator		*estimator.Estimator		`json:"RS"`
}

var proxy_handlers = map[string]*handler {
	"nobucket_upload": &handler{
		Params:	1,
		Methods: []string{"POST", "PUT"},
		Function: nobucket_upload_handler,
	},
	"upload": &handler{
		Params: 2,
		Methods: []string{"POST", "PUT"},
		Function: bucket_upload_handler,
	},
	"get": &handler{
		Params: 2,
		Methods: []string{"GET"},
		Function: get_handler,
	},
	"lookup": &handler{
		Params: 2,
		Methods: []string{"GET"},
		Function: lookup_handler,
	},
	"redirect": &handler{
		Params: 2,
		Methods: []string{"GET"},
		Function: redirect_handler,
	},
	"delete": &handler{
		Params: 2,
		Methods: []string{"POST", "PUT"},
		Function: delete_handler,
	},
	"bulk_delete": &handler{
		Params: 1,
		Methods: []string{"POST", "PUT"},
		Function: bulk_delete_handler,
	},
	"ping": &handler{
		Params: 0,
		Methods: []string{"GET"},
		Function: stat_handler,
	},
	"stat": &handler{
		Params: 0,
		Methods: []string{"GET"},
		Function: stat_handler,
	},
	"proxy_stat": &handler{
		Params: 0,
		Methods: []string{"GET"},
		Function: proxy_stat_handler,
	},
	"/": &handler{
		Params: 0,
		Methods: []string{"GET"},
		Function: common_handler,
	},
	"exit": &handler{
		Params:	0,
		Methods: []string{"GET"},
		Function: exit_handler,
	},
	"profile": &handler{
		Params:	0,
		Methods: []string{"GET"},
		Function: profile_handler,
	},
}

func get_content_length(header http.Header) uint64 {
	var content_length uint64 = 0

	lheader, ok := header["Content-Length"]
	if ok {
		var err error
		content_length, err = strconv.ParseUint(lheader[0], 0, 64)
		if err != nil {
			content_length = 0
		}
	}

	return content_length
}

func generic_handler(w http.ResponseWriter, req *http.Request) {
	// join together sequential // in the URL path

	start := time.Now()

	proxy.bctl.RLock()
	for k, v := range proxy.bctl.Conf.Proxy.Headers {
		w.Header().Set(k, v)
	}
	proxy.bctl.RUnlock()

	content_length := get_content_length(req.Header)

	reply := Reply {
		status: http.StatusBadRequest,
		err: errors.NewKeyError(req.URL.String(), http.StatusBadRequest, "there is no registered handler for this path"),
	}

	var h *handler = nil

	if req.Method == "HEAD" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return
	}

	path, err := url.QueryUnescape(req.URL.Path)
	if err != nil {
		path = req.URL.Path
		reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("could not unescape URL: %v", err))
	} else {
		hstrings := strings.SplitN(path, "/", 3)
		if len(hstrings) < 2 {
			path = req.URL.Path
			reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest, fmt.Sprintf("could not split URL"))
		} else {
			var ok bool

			param_strings := make([]string, 0)
			h, ok = proxy_handlers[hstrings[1]]
			if !ok {
				h = proxy_handlers["/"]
				param_strings = []string{path}
				ok = true
			} else {
				if len(hstrings) != 3 {
					reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
						fmt.Sprintf("not enough path parts for handler: %v, must be at least: %d",
							len(hstrings) - 1, h.Params + 1))
					ok = false
				} else {
					param_strings = strings.SplitN(hstrings[2], "/", h.Params)
					if len(param_strings) < h.Params {
						reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
							fmt.Sprintf("not enough path parameters for handler: %v, must be at least: %d",
								len(param_strings), h.Params))
						ok = false
					} else if h.Params > 0 && len(param_strings[h.Params - 1]) == 0 {
						reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
							fmt.Sprintf("last path parameter can not be empty"))
						ok = false
					}
				}
			}

			if ok {
				method_matched := false
				for _, method := range h.Methods {
					if method == req.Method {
						method_matched = true
						break
					}
				}

				if method_matched {
					reply = h.Function(w, req, param_strings...)
				} else {
					reply.err = errors.NewKeyError(req.URL.String(), http.StatusBadRequest,
						fmt.Sprintf("method doesn't match: provided: %s, required: %v",
							req.Method, h.Methods))
				}
			}
		}
	}

	msg := "OK"
	if reply.err != nil {
		msg = reply.err.Error()
		proxy.add_error(req.Method, req.RemoteAddr, req.URL.RequestURI(), reply.status, msg)
	}

	if content_length == 0 {
		content_length = get_content_length(w.Header())
		if content_length == 0 {
			content_length = reply.length
		}
	}

	duration := time.Since(start)
	if h != nil {
		h.Estimator.Push(content_length, reply.status)
	}

	log.Printf("access_log: method: '%s', client: '%s', x-fwd: '%v', path: '%s', encoded-uri: '%s', status: %d, size: %d, time: %.3f ms, err: '%v'\n",
		req.Method, req.RemoteAddr, req.Header.Get("X-Forwarded-For"),
		path, req.URL.RequestURI(), reply.status, content_length,
		float64(duration.Nanoseconds()) / 1000000.0, msg)

	if reply.err != nil {
		http.Error(w, reply.err.Error(), reply.status)
	}
}

func (proxy *bproxy) getTimeoutServer(addr string, handler http.Handler) *http.Server {
	//keeps people who are slow or are sending keep-alives from eating all our sockets
	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		// disable timeout checks, looks like these are not fair timeouts,
		// but instead maximum duration of the handler
		//ReadTimeout:  time.Duration(proxy.bctl.Conf.Proxy.IdleTimeout) * time.Second,
		//WriteTimeout:  time.Duration(proxy.bctl.Conf.Proxy.IdleTimeout) * time.Second,
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

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	for _, h := range proxy_handlers {
		h.Estimator = estimator.NewEstimator()
	}
	estimator_scan_handlers = proxy_handlers

	var err error

	conf := &config.ProxyConfig {}
	err = conf.Load(*config_file)
	if err != nil {
		log.Fatalf("Could not load config %s: %q", *config_file, err)
	}

	if *buckets == "" && len(conf.Elliptics.BucketList) == 0 {
		log.Fatalf("There is no buckets file and there is no 'bucket-list' option in elliptics config.")
	}


	if len(conf.Proxy.Address) == 0 {
		log.Fatalf("'address' must be specified in proxy config '%s'\n", *config_file)
	}

	if conf.Proxy.RedirectPort == 0 || conf.Proxy.RedirectPort >= 65536 {
		log.Printf("redirect is not allowed because of invalid redirect port %d",
			conf.Proxy.RedirectPort)
	}

	proxy.last_errors = make([]ErrorInfo, last_errors_length, last_errors_length)

	proxy.ell, err = etransport.NewEllipticsTransport(conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	rand.Seed(time.Now().Unix())

	proxy.bctl, err = bucket.NewBucketCtl(proxy.ell, *buckets, *config_file)
	if err != nil {
		log.Fatalf("Could not create new bucket controller: %v", err)
	}

	if len(conf.Proxy.HTTPSAddress) != 0 {
		if len(conf.Proxy.CertFile) == 0 {
			log.Fatalf("If you have specified HTTPS address there MUST be certificate file option")
		}

		if len(conf.Proxy.KeyFile) == 0 {
			log.Fatalf("If you have specified HTTPS address there MUST be key file option")
		}

		// this is needed to allow both HTTPS and HTTP handlers
		go func() {
			server := proxy.getTimeoutServer(proxy.bctl.Conf.Proxy.HTTPSAddress, http.HandlerFunc(generic_handler))
			log.Fatal(server.ListenAndServeTLS(conf.Proxy.CertFile, conf.Proxy.KeyFile))
		}()
	}

	if len(conf.Proxy.Address) != 0 {
		server := proxy.getTimeoutServer(proxy.bctl.Conf.Proxy.Address, http.HandlerFunc(generic_handler))
		log.Fatal(server.ListenAndServe())
	}
}
