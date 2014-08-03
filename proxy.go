package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/elliptics"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/rift"
	"github.com/bioothod/backrunner/transport"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
)

var (
	proxy bproxy

	upload_prefix string = "/upload/"
	ping_prefix   string = "/ping/"
)

type bproxy struct {
	host		string
	bctl		bucket.BucketCtl
	transport	transport.Transport
}

func (p *bproxy) local_url(key, bucket, operation string) string {
	return fmt.Sprintf("http://%s/%s/%s/%s", p.host, operation, bucket, key)
}

func upload_handler(w http.ResponseWriter, http_req *http.Request) {
	var req transport.Request
	req.Http = http_req
	req.Key = http_req.URL.Path[len(upload_prefix):]
	req.Bctl = &proxy.bctl
	req.Bucket = proxy.bctl.GetBucket()

	var err error
	req.User, err = req.Bctl.CheckAuth(http_req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	resp, err := proxy.transport.Upload(&req)
	if resp.Status != http.StatusOK {
		log.Printf("url: %s: transport error: %d", http_req.URL, resp.Status)
		req.Bucket.HalfRate()

		err = errors.NewKeyError(http_req.URL.String(), resp.Status, string(resp.Data))
		http.Error(w, err.Error(), resp.Status)
		return
	}

	m := resp.Json.(map[string]interface{})
	rate := m["rate"]
	if rate != nil {
		req.Bucket.SetRate(rate.(float64))
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
		Bucket: req.Bucket.Name,
		Reply:  &resp.Json,
		Primary: ent_reply{
			Key:    req.Key,
			Get:    "GET " + proxy.local_url(req.Key, req.Bucket.Name, "get"),
			Update: "POST " + proxy.local_url(req.Key, req.Bucket.Name, "upload"),
			Delete: "POST " + proxy.local_url(req.Key, req.Bucket.Name, "delete"),
		},
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: upload: json marshal failed: %q\n", http_req.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reply_json)
}

func ping_handler(w http.ResponseWriter, r *http.Request) {
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

func generic_handler(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, ping_prefix) {
		ping_handler(w, req)
		return
	}

	if strings.HasPrefix(req.URL.Path, upload_prefix) {
		upload_handler(w, req)
		return
	}
}

func getTimeoutServer(addr string, handler http.Handler) *http.Server {
	//keeps people who are slow or are sending keep-alives from eating all our sockets
	const (
		HTTP_READ_TO  = transport.DEFAULT_IDLE_TIMEOUT
		HTTP_WRITE_TO = transport.DEFAULT_IDLE_TIMEOUT
	)

	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  HTTP_READ_TO,
		WriteTimeout: HTTP_WRITE_TO,
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

	var remotes stringslice
	flag.Var(&remotes, "remote", "connect to the RIFT proxy on given address in the following format: address:port")
	listen := flag.String("listen", "0.0.0.0:9090", "listen and serve address")
	buckets := flag.String("buckets", "", "buckets file (file format: new-line separated list of bucket names)")
	acl := flag.String("acl", "", "ACL file in the same JSON format as RIFT buckets")
	config := flag.String("config", "", "Transport config file")
	tname := flag.String("transport", "rift", "Transport name: rift or elliptics")
	flag.Parse()

	if *buckets == "" {
		log.Fatal("there is no buckets file")
	}

	if *acl == "" {
		log.Fatal("there is no ACL file")
	}

	var err error
	if *tname == "rift" {
		if len(remotes) == 0 {
			log.Fatal("No remote nodes specified")
		}

		proxy.transport, err = rift.NewRiftTransport(remotes)
	} else if *tname == "elliptics" {
		if *config == "" {
			log.Fatal("You must specify config file")
		}
		proxy.transport, err = elliptics.NewEllipticsTransport(*config)
	} else {
		log.Fatalf("Unsupported transport name '%s'", *tname)
	}

	if err != nil {
		log.Fatalf("Could not create %s transport", *tname, err)
	}

	rand.Seed(9)

	proxy.bctl, err = bucket.NewBucketCtl(remotes, *buckets, *acl)
	if err != nil {
		log.Fatal("Could not process buckets file '"+*buckets+"'", err)
	}

	proxy.host = remotes[0]


	server := getTimeoutServer(*listen, http.HandlerFunc(generic_handler))

	log.Fatal(server.ListenAndServe())
}
