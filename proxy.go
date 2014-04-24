package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"io/ioutil"
	"math/rand"
	"net/http"
)

var (
	proxy bproxy
	buckets []string = []string{"bucket:11.21", "bucket:22.31", "bucket:32.12"}

	put_prefix string = "/put/"
)

type KeyError struct {
	url string
	status int
	data []byte
}

func (k *KeyError) Error() string {
	return fmt.Sprintf("url: %s: error code: %d, returned data: '%s'", k.url, k.status, fmt.Sprintf("%s", k.data))
}

func NewKeyError(url string, status int, data []byte) error {
	return &KeyError{
		url: url,
		status: status,
		data: data,
	}
}

type bproxy struct {
	host string
	client *http.Client
}

func (p *bproxy) upload_one(key, bucket string, data []byte) (ret []byte, err error) {
	url := fmt.Sprintf("%s/upload/%s?bucket=%s", p.host, key, bucket)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		log.Printf("url: %s: new request failed: %q", url, err)
		return
	}

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("url: %s: post failed: %q", url, err)
		return
	}
	defer resp.Body.Close()

	// encode JSON
	ret, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("url: %s: readall response failed: %q", url, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = NewKeyError(url, resp.StatusCode, ret)
		log.Printf("%s", err)
		return
	}

	log.Printf("%s\n", NewKeyError(url, resp.StatusCode, ret))
	return
}

func (p *bproxy) remove_one(key, bucket string) (ret []byte, err error) {
	return
}

func put_handler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("readall failed: %q", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	bucket := buckets[rand.Intn(len(buckets))]
	key := r.URL.Path[len(put_prefix):]

	ret_primary, err := proxy.upload_one(key, bucket, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	backup_key := key + ".backup"
	ret_backup, err := proxy.upload_one(backup_key, bucket, data)
	if err != nil {
		_, _ = proxy.remove_one(key, bucket)

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	type ent_reply struct {
		Key, Reply string
	}
	type put_reply struct {
		Primary, Backup ent_reply
	}

	reply := put_reply{
		Primary: ent_reply{
			Key: key,
			Reply: fmt.Sprintf("%s", ret_primary),
		},
		Backup: ent_reply{
			Key: backup_key,
			Reply: fmt.Sprintf("%s", ret_backup),
		},
	}

	reply_json, err := json.Marshal(reply)
	if err != nil {
		_, _ = proxy.remove_one(key, bucket)
		_, _ = proxy.remove_one(backup_key, bucket)

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Error(w, fmt.Sprintf("%s", reply_json), http.StatusOK)
}

func main() {
	rand.Seed(9)

	proxy.client = &http.Client{}
	proxy.host = "http://108.61.155.67:80"

	http.HandleFunc(put_prefix, put_handler)

	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
