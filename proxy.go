package main

import (
	//"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

const DEFAULT_IDLE_TIMEOUT = 5 * time.Second

var (
	proxy bproxy

	upload_prefix string = "/upload/"
	ping_prefix   string = "/ping/"

	auth_header_str string = "Authorization"
)

type KeyError struct {
	url    string
	status int
	data   []byte
}

func (k *KeyError) Error() string {
	return fmt.Sprintf("url: %s: error code: %d, returned data: '%s'", k.url, k.status, fmt.Sprintf("%s", k.data))
}

func NewKeyError(url string, status int, data []byte) (err error) {
	err = &KeyError{
		url:    url,
		status: status,
		data:   data,
	}
	log.Printf("%s", err)
	return
}

type bproxy struct {
	host   string
	client *http.Client
	acl    map[string]BucketACL
}

type request struct {
	proxy *bproxy
	url   string
	user  string
	query url.Values
	data  io.Reader

	reply  []byte
	status int
}

func (p *bproxy) proxy_request(req *http.Request) (new_req request) {
	new_req = request{
		proxy:  p,
		url:    "",
		query:  req.URL.Query(),
		data:   nil,
		reply:  nil,
		status: http.StatusBadRequest,
	}

	return
}

func (p *bproxy) generate_url(key, bucket, operation string) string {
	return fmt.Sprintf("http://%s/%s/%s/%s", p.host, operation, bucket, key)
}

func (p *bproxy) generate_signature(user string, r *http.Request) (sign string, err error) {
	acl, ok := p.acl[user]
	if !ok {
		err = NewKeyError(r.URL.String(), http.StatusForbidden, []byte(fmt.Sprintf("url: %s: there is no user '%s' in ACL\n", r.URL, user)))
		return
	}

	sign, err = GenerateSignature(acl.token, r.Method, r.URL, r.Header)
	if err != nil {
		err = NewKeyError(r.URL.String(), http.StatusForbidden, []byte(fmt.Sprintf("url: %s: hmac generation failed: %s\n", r.URL, err)))
		return
	}

	return
}

func (p *bproxy) auth_check(r *http.Request) (user string, err error) {
	user = ""
	err = nil
	if p.acl == nil {
		return
	}

	auth_headers, ok := r.Header[auth_header_str]
	if !ok {
		err = NewKeyError(r.URL.String(), http.StatusForbidden, []byte(fmt.Sprintf("url: %s: there is no '%s' header\n", r.URL, auth_header_str)))
		return
	}

	auth_data := strings.Split(auth_headers[0], " ")
	if len(auth_data) != 2 {
		err = NewKeyError(r.URL.String(), http.StatusForbidden, []byte(fmt.Sprintf("url: %s: auth header1 '%s' must be 'riftv1 user:hmac'\n",
			r.URL, auth_headers[0])))
		return
	}

	auth_data = strings.Split(auth_data[1], ":")
	if len(auth_data) != 2 {
		err = NewKeyError(r.URL.String(), http.StatusForbidden, []byte(fmt.Sprintf("url: %s: auth header2 '%s' must be 'riftv1 user:hmac'\n",
			r.URL, auth_headers[0])))
		return
	}

	user = auth_data[0]
	recv_auth := auth_data[1]

	calc_auth, err := p.generate_signature(user, r)
	if err != nil {
		return
	}

	if recv_auth != calc_auth {
		err = NewKeyError(r.URL.String(), http.StatusForbidden,
			[]byte(fmt.Sprintf("url: %s: hmac mismatch: recv: '%s', calc: '%s'\n", r.URL, recv_auth, calc_auth)))
		return
	}

	return user, nil
}

func upload_handler(w http.ResponseWriter, req *http.Request) {
	user, err := proxy.auth_check(req)
	if err != nil {
		log.Printf("url: %s: upload: auth check failed: %q\n", req.URL, err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	bucket := Buckets[rand.Intn(len(Buckets))]
	key := req.URL.Path[len(upload_prefix):]

	req.URL, err = url.Parse(proxy.generate_url(key, bucket, "upload"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sign, err := proxy.generate_signature(user, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	req.Header[auth_header_str] = []string{"riftv1 " + user + ":" + sign}

	req.RequestURI = ""
	resp, err := proxy.client.Do(req)
	if err != nil {
		log.Printf("url: %s: post failed: %q", req.URL, err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	rift_reply, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("url: %s: readall response failed: %q", req.URL, err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = NewKeyError(req.URL.String(), resp.StatusCode, rift_reply)
		http.Error(w, err.Error(), resp.StatusCode)
		return
	}

	var rift_json interface{}
	err = json.Unmarshal(rift_reply, &rift_json)
	if err != nil {
		rift_json = nil

		log.Printf("url: %s: upload: can not unmarshall rift reply: '%s', error: %q\n", req.URL, rift_reply, err)
	}

	query := ""
	if len(req.URL.RawQuery) != 0 {
		query = "?" + req.URL.RawQuery
	}

	if len(req.URL.Fragment) != 0 {
		query += "#" + req.URL.Fragment
	}

	type ent_reply struct {
		Get    string `json:"get"`
		Update string `json:"update"`
		Delete string `json:"delete"`
		Key    string `json:"key"`
	}
	type upload_reply struct {
		Bucket  string       `json:"bucket"`
		Primary ent_reply    `json:"primary"`
		Reply   *interface{} `json:"reply"`
	}

	reply := upload_reply{
		Bucket: bucket,
		Reply:  &rift_json,
		Primary: ent_reply{
			Key:    key,
			Get:    "GET " + proxy.generate_url(key, bucket, "get"),
			Update: "POST " + req.URL.String() + query,
			Delete: "POST " + proxy.generate_url(key, bucket, "delete"),
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

func ping_handler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Ping OK", http.StatusOK)
}

type stringslice []string

func (str *stringslice) String() string {
	return fmt.Sprintf("%d", *str)
}

func (str *stringslice) Set(value string) error {
	*str = append(*str, value)
	return nil
}

func getTimeoutServer(addr string, handler http.Handler) *http.Server {
	//keeps people who are slow or are sending keep-alives from eating all our sockets
	const (
		HTTP_READ_TO  = DEFAULT_IDLE_TIMEOUT
		HTTP_WRITE_TO = DEFAULT_IDLE_TIMEOUT
	)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  HTTP_READ_TO,
		WriteTimeout: HTTP_WRITE_TO,
	}
}

func NoProxyAllowed(request *http.Request) (*url.URL, error) {
	return nil, nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var remotes stringslice
	flag.Var(&remotes, "remote", "connect to the RIFT proxy on given address in the following format: address:port")
	listen := flag.String("listen", "0.0.0.0:9090", "listen and serve address")
	buckets := flag.String("buckets", "", "buckets file (file format: new-line separated list of bucket names)")
	acl := flag.String("acl", "", "ACL file in the same JSON format as RIFT buckets")
	flag.Parse()

	if len(remotes) == 0 {
		log.Fatal("no remote nodes specified")
	}

	if *buckets == "" {
		log.Fatal("there is no buckets file")
	}

	err := BucketsInit(*buckets)
	if err != nil {
		log.Fatal("Could not process buckets file '"+*buckets+"'", err)
	}

	rand.Seed(9)

	proxy.client = &http.Client{
		Transport: &http.Transport{
			Proxy: NoProxyAllowed,
			MaxIdleConnsPerHost: 1024,
			DisableKeepAlives: false,
			DisableCompression: false,
			Dial: func(network, addr string) (net.Conn, error) {
				return NewTimeoutConnDial(network, addr, DEFAULT_IDLE_TIMEOUT)
			},
		},
	}
	proxy.host = remotes[0]
	proxy.acl = nil

	if *acl != "" {
		var err error
		proxy.acl, err = BucketACL_Extract_JSON_File(*acl)
		if err != nil {
			log.Fatal("ACL: ", err)
		}
	}

	server := getTimeoutServer(*listen, http.HandlerFunc(upload_handler))

	log.Fatal(server.ListenAndServe())
}
