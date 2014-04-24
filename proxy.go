package main

import (
	"bytes"
	"fmt"
	"log"
	"io/ioutil"
	"net/http"
)

var (
	proxy bproxy
	buckets []string
)

type bproxy struct {
	host string
	client *http.Client
}

func put_handler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("readall failed: %q", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	bucket := "123"
	key := "key"
	uri := "qwe&asd=111"

	url := fmt.Sprintf("%s/upload/%s/%s?%s", proxy.host, bucket, key, uri)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		log.Printf("new request failed: %q", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := proxy.client.Do(req)
	if err != nil {
		log.Printf("post failed: %q", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer resp.Body.Close()
	// encode JSON
	_, err = ioutil.ReadAll(resp.Body)
	return
}

func main() {
	proxy.client = &http.Client{}
	proxy.host = "http://108.61.155.67:80"

	http.HandleFunc("/put/", put_handler)

	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
