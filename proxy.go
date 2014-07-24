package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	proxy bproxy

	upload_prefix string = "/upload/"
	delete_prefix string = "/delete/"
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

func NewKeyError(url string, status int, data []byte) error {
	return &KeyError{
		url:    url,
		status: status,
		data:   data,
	}
}

type bproxy struct {
	host   string
	client *http.Client
	backup bool
	acl    map[string]BucketACL
}

type request struct {
	proxy *bproxy
	url string
	user string
	query url.Values
	data []byte

	reply []byte
	status int
}

func (p *bproxy)proxy_request(req *http.Request) (new_req request) {
	new_req = request{
		proxy: p,
		url: "",
		query: req.URL.Query(),
		data: nil,
		reply: nil,
		status: http.StatusBadRequest,
	}

	return
}

func (r *request) send() (err error) {
	buf := bytes.NewBuffer([]byte{})

	if r.data != nil {
		buf = bytes.NewBuffer(r.data)
	}

	req, err := http.NewRequest("POST", r.url, buf)
	if err != nil {
		r.status = http.StatusPreconditionFailed
		log.Printf("url: %s: new request failed: %q", r.url, err)
		return
	}

	req.URL.RawQuery = r.query.Encode()
	sign, err := r.proxy.generate_signature(r.user, req)
	if err != nil {
		r.status = http.StatusForbidden
		return
	}

	req.Header[auth_header_str] = []string{"riftv1 " + r.user + ":" + sign}

	resp, err := r.proxy.client.Do(req)
	if err != nil {
		r.status = http.StatusTeapot
		log.Printf("url: %s: post failed: %q", req.URL, err)
		return
	}
	defer resp.Body.Close()

	r.status = resp.StatusCode

	r.reply, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("url: %s: readall response failed: %q", req.URL, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = NewKeyError(req.URL.String(), resp.StatusCode, r.reply)
		log.Printf("%s", err)
		return
	}

	return
}

func (p *bproxy) backup_key(key string) string {
	return key + ".backup"
}

func generate_url(host, key, bucket, operation string) string {
	return fmt.Sprintf("http://%s/%s/%s/%s", host, operation, bucket, key)
}

func (p *bproxy) generate_url(key, bucket, operation string) string {
	return generate_url(p.host, key, bucket, operation)
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

func upload_handler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	user, err := proxy.auth_check(r)
	if err != nil {
		log.Printf("url: %s: upload: auth check failed: %q\n", r.URL, err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("url: %s: upload: readall failed: %q\n", r.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	bucket := Buckets[rand.Intn(len(Buckets))]
	key := r.URL.Path[len(upload_prefix):]

	req := proxy.proxy_request(r)
	req.url = proxy.generate_url(key, bucket, "upload")
	req.data = data
	req.user = user

	err = req.send()
	if err != nil {
		log.Printf("url: %s: upload: send failed: %q\n", r.URL, err)
		http.Error(w, err.Error(), req.status)
		return
	}

	query := "?" + req.query.Encode()
	if len(query) == 1 {
		query = ""
	}

	type ent_reply struct {
		Get    string `json:"get"`
		Update string `json:"update"`
		Delete string `json:"delete"`
		Key    string `json:"key"`
		Reply  string `json:"reply"`
	}
	type upload_reply struct {
		Bucket  string    `json:"bucket"`
		Primary ent_reply `json:"primary"`
		Backup  ent_reply `json:"backup"`
	}

	reply := upload_reply{
		Bucket: bucket,
		Primary: ent_reply{
			Key:    key,
			Get:    "GET " + proxy.generate_url(key, bucket, "get"),
			Update: "POST " + req.url + query,
			Delete: "POST " + generate_url(r.Host, key, bucket, "delete") + query,
			Reply:  string(req.reply),
		},
	}

	backup_key := proxy.backup_key(key)
	if proxy.backup {
		req.url = proxy.generate_url(backup_key, bucket, "upload")
		err = req.send()
		if err != nil {
			log.Printf("url: %s: upload: backup send failed: %q\n", r.URL, err)

			req.url = proxy.generate_url(key, bucket, "delete")
			req.send()

			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reply.Backup = ent_reply{
			Key:   backup_key,
			Get:   "GET " + proxy.generate_url(backup_key, bucket, "get"),
			Reply: string(req.reply),
		}
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		log.Printf("url: %s: upload: json marshal failed: %q\n", r.URL, err)

		req.url = proxy.generate_url(key, bucket, "delete")
		req.send()

		if proxy.backup {
			req.url = proxy.generate_url(backup_key, bucket, "delete")
			req.send()
		}

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Error(w, string(reply_json), http.StatusOK)
}

func delete_handler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	user, err := proxy.auth_check(r)
	if err != nil {
		log.Printf("url: %s: delete: auth check failed: %q\n", r.URL, err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	pc := strings.Split(r.URL.Path, "/")
	if len(pc) < 2 {
		tmp := fmt.Sprintf("url: %s: delete: invalid URL, there must be at least 2 components in the path\n", r.URL)
		log.Printf("%s\n", tmp)
		http.Error(w, tmp, http.StatusBadRequest)
		return
	}

	bucket := pc[2]
	key := strings.Join(pc[3:], "/")

	req := proxy.proxy_request(r)
	req.url = proxy.generate_url(key, bucket, "delete")
	req.user = user

	err = req.send()
	if err != nil {
		log.Printf("url: %s: delete: delete request failed: %q\n", r.URL, err)

		if req.status == http.StatusOK {
			req.status = http.StatusBadRequest
		}

		str := string(req.reply) + "\n" + err.Error()
		http.Error(w, str, req.status)
		return
	}

	if !proxy.backup {
		http.Error(w, "Successfully removed key '"+key+"': "+string(req.reply), http.StatusOK)
		return
	}

	backup_key := proxy.backup_key(key)

	type update struct {
		Id      string            `json:"id"`
		Indexes map[string][]byte `json:"indexes"`
	}

	entry := &Delentry{
		time: time.Now().Add(2 * 24 * 3600 * time.Second).Unix(),
		key:  backup_key,
	}

	index_data, err := entry.pack()
	if err != nil {
		err := NewKeyError(r.URL.String(), http.StatusBadRequest, []byte(fmt.Sprintf("could not pack delete entry: %v", err)))
		log.Printf("url: %s: delete: pack failed: %q", r.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	update_json := update{
		Id: backup_key,
		Indexes: map[string][]byte{
			DeleteIndex: index_data,
		},
	}

	req.url = proxy.generate_url(backup_key, bucket, "update")
	req.data, err = json.Marshal(update_json)
	if err != nil {
		log.Printf("url: %s: delete: json marshal failed: %q", r.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = req.send()
	if err != nil {
		log.Printf("url: %s: delete: backup index update send failed: %q", r.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Error(w, string(req.reply), http.StatusOK)
}

func ping_handler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Ping OK", http.StatusOK)
}

func main() {
	remote := flag.String("remote", "108.61.155.67:80", "connect to the RIFT proxy on given address in the following format: address:port")
	listen := flag.String("listen", ":9090", "listen and serve address")
	buckets := flag.String("buckets", "", "buckets file (file format: new-line separated list of bucket names)")
	backup := flag.Bool("backup", false, "enable backup copy")
	acl := flag.String("acl", "", "ACL file in the same JSON format as RIFT buckets")
	flag.Parse()

	if *buckets == "" {
		log.Fatal("there is no bucket file")
	}

	err := BucketsInit(*buckets)
	if err != nil {
		log.Fatal("Buckets file '" + *buckets + "'", err)
	}

	rand.Seed(9)

	proxy.client = &http.Client{}
	proxy.host = *remote
	proxy.backup = *backup
	proxy.acl = nil

	if *acl != "" {
		var err error
		proxy.acl, err = BucketACL_Extract_JSON_File(*acl)
		if err != nil {
			log.Fatal("ACL: ", err)
		}
	}

	http.HandleFunc(upload_prefix, upload_handler)
	http.HandleFunc(delete_prefix, delete_handler)
	http.HandleFunc(ping_prefix, ping_handler)

	err = http.ListenAndServe(*listen, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
