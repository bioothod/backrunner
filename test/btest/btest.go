package btest

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"github.com/bioothod/elliptics-go/elliptics"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"syscall"
	"time"
	"reflect"
	"runtime"
	"strconv"
)

type CheckRequest struct {
	request *http.Request
	status int
}

type BackrunnerTest struct {
	base string

	// various log files
	test_log, server_log, proxy_log string

	// file where all IO bucket names are stored, it is used by proxy
	bucket_file string

	server_cmd *exec.Cmd
	proxy_cmd *exec.Cmd

	elliptics_address []string

	client *http.Client

	ell *etransport.Elliptics

	conf *config.ProxyConfig

	// bucket which contains group, which does not exist in the elliptics cluster
	failed_bucket string

	// buffer used in ACL check requests, it is never freed
	acl_buffer []byte

	// test bucket with wide variety of ACLs
	acl_bucket string

	groups []uint32

	// test key used to for reading/writing/deleting ACL checks
	// it is first uploaded via Elliptics API in every ACL test,
	// this is needed to allow /get/ handler checks in case when /upload/ is forbidden
	acl_key string

	// array of request/response-status pairs for different ACL checks
	acl_requests []*CheckRequest

	// this user is used for non-ACL tests (big/small uploads, data consistency and so on)
	all_allowed_user string
	all_allowed_token string

	// array of test buckets used for load balancing and IO tests
	// each bucket matches one group in @groups
	io_buckets []string

	// uniform free space test will write data until elliptics returns error
	// it should be 'no space' error
	// proxy should write data uniformly among all buckets and backends
	//
	// this value will be specified in proxy config, it sets minimum ratio of free
	// space for every backend, it is forbidden to write into backend if amount of free
	// space will be less than hard limit, it is only allowed to write into bucket
	// with less than soft free space ratio limit if there are no other buckets to write
	free_space_ratio_soft float64
	free_space_ratio_hard float64
}

func (t *BackrunnerTest) check_upload_reply(bucket, key string, resp *http.Response) (*reply.Upload, error) {
	var err error

	resp_data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status: '%s', url: '%s', headers: req: %v, resp: %v, data-received: %s",
			resp.Status, resp.Request.URL.String(), resp.Request.Header, resp.Header, string(resp_data))
	}

	var rep reply.Upload
	err = json.Unmarshal(resp_data, &rep)
	if err != nil {
		return nil, fmt.Errorf("invalid reply '%s': %s", string(resp_data), err.Error())
	}

	if rep.Key != key {
		return nil, fmt.Errorf("invalid reply '%s': keys do not match: sent: '%s', recv: '%s'", string(resp_data), key, rep.Key)
	}

	if rep.Bucket == "" {
		return nil, fmt.Errorf("invalid reply '%s': returned invalid bucket name: '%s'", string(resp_data), rep.Bucket)
	}

	if bucket != "" {
		if rep.Bucket != bucket {
			return nil, fmt.Errorf("invalid reply '%s': buckets do not match: sent: %s, recv: %s",
				string(resp_data), bucket, rep.Bucket)
		}
	}

	return &rep, nil
}

func (t *BackrunnerTest) NewRequest(method, handler, user, token, bucket, key string, offset, size uint64, body io.Reader) *http.Request {
	url := fmt.Sprintf("http://%s/%s", t.conf.Proxy.Address, handler)

	if bucket != "" {
		url = fmt.Sprintf("%s/%s", url, bucket)
	}
	if key != "" {
		url = fmt.Sprintf("%s/%s", url, key)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Fatal("Could not create request: method: %s, url: '%s': %v\n", method, url, err)
	}

	if offset != 0 || size != 0 {
		q := req.URL.Query()
		if offset != 0 {
			q.Set("offset", strconv.FormatUint(offset, 10))
		}
		if size != 0 {
			q.Set("size", strconv.FormatUint(size, 10))
		}

		req.URL.RawQuery = q.Encode()
	}

	if user != "" && token != "" {
		sign, err := auth.GenerateSignature(token, req.Method, req.URL, req.Header)
		if err != nil {
			log.Fatal("Could not generate signature: token: '%s', method: %s, url: %s, header: %v: %v\n",
				token, req.Method, req.URL.String(), req.Header, err)
		}

		req.Header.Add(auth.AuthHeaderStr, fmt.Sprintf("riftv1 %s:%s", user, sign))
	}

	return req
}

func (t *BackrunnerTest) NewEmptyRequest(method, handler, user, token, bucket, key string) *http.Request {
	return t.NewRequest(method, handler, user, token, bucket, key, 0, 0, bytes.NewReader([]byte{}))
}

func (t *BackrunnerTest) NewCheckRequest(method, handler, user, token string, status int) *CheckRequest {
	body := bytes.NewReader(t.acl_buffer)

	ret := &CheckRequest {
		request: t.NewRequest(method, handler, user, token, t.acl_bucket, t.acl_key, 0, 0, body),
		status: status,
	}

	log.Printf("method: %s, handler: %s, user: %s, token: %s, url: %s, headers: %v\n",
		method, handler, user, token, ret.request.URL.String(), ret.request.Header)

	return ret
}

func test_common_read(t *BackrunnerTest) error {
	req := t.NewEmptyRequest("GET", "", "", "", "", "server.log")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("common: url: %s: could not send get request: %v", req.URL.String(), err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("common: url: %s: could not read reply: %v", req.URL.String(), req.Header, err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("check-content: url: %s, returned status: %d, must be: %d, data: %s",
			req.URL.String(), resp.StatusCode, http.StatusOK, string(data))
	}

	return nil
}


func (t *BackrunnerTest) ACLInit() error {
	user := strconv.FormatInt(rand.Int63(), 16)

	meta := bucket.BucketMsgpack {
		Version: 1,
		Name: t.acl_bucket,
		Groups: t.groups,
		Acl: make(map[string]bucket.BucketACL),
	}

	var acl bucket.BucketACL
	var flags uint64

	flags = bucket.BucketAuthEmpty
	acl = bucket.BucketACL {
		Version: 1,
		User: fmt.Sprintf("user-%s-%x", user, flags),
		Token: strconv.FormatInt(rand.Int63(), 16),
		Flags: flags,
	}
	meta.Acl[acl.User] = acl
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, acl.Token, http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, acl.Token, http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "", "", http.StatusForbidden))


	flags = bucket.BucketAuthNoToken
	acl = bucket.BucketACL {
		Version: 1,
		User: fmt.Sprintf("user-%s-%x", user, flags),
		Token: strconv.FormatInt(rand.Int63(), 16),
		Flags: flags,
	}
	meta.Acl[acl.User] = acl
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, "qwerty", http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, acl.Token, http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, acl.Token, http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "", "", http.StatusForbidden))

	flags = bucket.BucketAuthWrite
	acl = bucket.BucketACL {
		Version: 1,
		User: fmt.Sprintf("user-%s-%x", user, flags),
		Token: strconv.FormatInt(rand.Int63(), 16),
		Flags: flags,
	}
	meta.Acl[acl.User] = acl
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "", "", http.StatusForbidden))

	flags = bucket.BucketAuthWrite | bucket.BucketAuthNoToken
	acl = bucket.BucketACL {
		Version: 1,
		User: fmt.Sprintf("user-%s-%x", user, flags),
		Token: strconv.FormatInt(rand.Int63(), 16),
		Flags: flags,
	}
	meta.Acl[acl.User] = acl
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", acl.User, "qwerty", http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("GET", "get", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", acl.User, "qwerty", http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "upload", "", "", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, acl.Token, http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", acl.User, "qwerty", http.StatusOK))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "qwerty", "qwerty", http.StatusForbidden))
	t.acl_requests = append(t.acl_requests, t.NewCheckRequest("POST", "delete", "", "", http.StatusForbidden))

	acl = bucket.BucketACL {
		Version: 1,
		User: t.all_allowed_user,
		Token: t.all_allowed_token,
		Flags: bucket.BucketAuthWrite | bucket.BucketAuthNoToken,
	}
	meta.Acl[acl.User] = acl

	_, err := bucket.WriteBucket(t.ell, &meta)
	if err != nil {
		log.Fatalf("Could not upload bucket: %v", err)
	}

	return nil
}

func test_acl(t *BackrunnerTest) error {
	for _, req := range t.acl_requests {
		// first, upload acl test key using Elliptics API to be able to run /get/ tests
		s, err := t.ell.DataSession(req.request)
		if err != nil {
			return fmt.Errorf("url: %s: could not create data session: %v", req.request.URL.String(), err)
		}
		s.SetNamespace(t.acl_bucket)
		s.SetGroups(t.groups)
		s.SetTimeout(100)

		for l := range s.WriteData(t.acl_key, bytes.NewReader(t.acl_buffer), 0, uint64(len(t.acl_buffer))) {
			if l.Error() != nil {
				return fmt.Errorf("url: %s: could not upload key '%s': %v", req.request.URL.String(), t.acl_key, l.Error())
			}
		}

		resp, err := t.client.Do(req.request)
		if err != nil {
			return fmt.Errorf("do: url: %s, headers: %v: %v", req.request.URL.String(), req.request.Header, err)
		}
		defer resp.Body.Close()

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("readall: url: %s, headers: %v: %v", req.request.URL.String(), req.request.Header, err)
		}

		if resp.StatusCode != req.status {
			return fmt.Errorf("status: url: %s, headers: %v: returned status: %d, must be: %d, data: %s",
				req.request.URL.String(), req.request.Header, resp.StatusCode, req.status, string(data))
		}
	}

	return nil
}

func (t *BackrunnerTest) check_key_content(bucket, key, user, token string, offset, size uint64, content []byte) error {
	req := t.NewRequest("GET", "get", user, token, bucket, key, offset, size, bytes.NewReader([]byte{}))

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("check-content: url: %s: could not send get request: %v", req.URL.String(), err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("check-content: url: %s: could not read reply: %v", req.URL.String(), req.Header, err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("check-content: url: %s, returned status: %d, must be: %d, data: %s",
			req.URL.String(), resp.StatusCode, http.StatusOK, string(data))
	}

	if !bytes.Equal(data, content) {
		first := 0
		last := 16
		if last > len(content) {
			last = len(content)
		}

		return fmt.Errorf("check-content: url: %s, different content: requested[%d:%d]: %s, len: %d, received: %s, len: %d",
			req.URL.String(), first, last,
			hex.Dump(data[first: last]), len(data),
			hex.Dump(content[first: last]), len(content))
	}

	return nil
}

func test_big_bucket_upload(t *BackrunnerTest) error {
	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]
	key := strconv.FormatInt(rand.Int63(), 16)

	// [20, 20+25) megabytes
	total_size := 1024 * (rand.Int31n(25 * 1024) + 20 * 1024)
	buf := make([]byte, total_size)
	_, err := cryptorand.Read(buf)
	if err != nil {
		return fmt.Errorf("big-bucket-upload: could not read random data: %v", err)
	}

	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "upload", t.all_allowed_user, t.all_allowed_token, bucket, key, 0, 0, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("big-bucket-upload: could not send upload request: %v", err)
	}
	defer resp.Body.Close()

	_, err = t.check_upload_reply(bucket, key, resp)
	if err != nil {
		return fmt.Errorf("big-bucket-upload: %v", err)
	}

	err = t.check_key_content(bucket, key, t.all_allowed_user, t.all_allowed_token, 0, uint64(total_size), buf)
	if err != nil {
		return fmt.Errorf("big-bucket-upload: full size: %d: %v", total_size, err)
	}

	offset := total_size / 2
	size := total_size / 4
	err = t.check_key_content(bucket, key, t.all_allowed_user, t.all_allowed_token,
		uint64(offset), uint64(size), buf[offset: offset + size])
	if err != nil {
		return fmt.Errorf("big-bucket-upload: offset: %d, size: %d, %v", offset, size, err)
	}

	return nil
}

func (t *BackrunnerTest) upload_get_helper(bucket, key_orig, user, token string) error {
	key := url.QueryEscape(key_orig)

	// [1, 1+100) kbytes
	total_size := 1024 * (rand.Int31n(100) + 1)
	buf := make([]byte, total_size)
	_, err := cryptorand.Read(buf)
	if err != nil {
		return fmt.Errorf("upload-get-helper: could not read random data: %v", err)
	}

	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "upload", user, token, bucket, key, 0, 0, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("update-get-helper: could not send upload request: %v", err)
	}
	defer resp.Body.Close()

	_, err = t.check_upload_reply(bucket, key_orig, resp)
	if err != nil {
		return fmt.Errorf("upload-get-helper: %v", err)
	}

	err = t.check_key_content(bucket, key, user, token, 0, uint64(total_size), buf)
	if err != nil {
		return fmt.Errorf("upload-get-helper: %v", err)
	}

	return nil
}

func test_small_bucket_upload(t *BackrunnerTest) error {
	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]
	key := "тестовый ключ :.&*^//$@#qweqфывфв0x44"

	return t.upload_get_helper(bucket, key, t.all_allowed_user, t.all_allowed_token)
}

func test_bucket_delete(t *BackrunnerTest) error {
	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]
	key := strconv.FormatInt(rand.Int63(), 16)

	// [1, 1+100) kbytes
	total_size := 1024 * (rand.Int31n(100) + 1)
	buf := make([]byte, total_size)
	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "upload", t.all_allowed_user, t.all_allowed_token, bucket, key, 0, 0, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("delete: url: %s: could not send initial upload request: %v", req.URL.String(), err.Error())
	}
	defer resp.Body.Close()

	_, err = t.check_upload_reply(bucket, key, resp)
	if err != nil {
		return err
	}

	req = t.NewRequest("POST", "delete", t.all_allowed_user, t.all_allowed_token, bucket, key, 0, 0, body)
	dresp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("delete: url: %s: could not send delete request: %v", req.URL.String(), err.Error())
	}
	defer dresp.Body.Close()

	data, err := ioutil.ReadAll(dresp.Body)
	if err != nil {
		return fmt.Errorf("delete: url: %s: could not read response body: %v", req.URL.String(), err.Error())
	}

	if dresp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete: url: %s: could not delete key: returned data: %s", req.URL.String(), string(data))
	}

	req = t.NewEmptyRequest("GET", "get", t.all_allowed_user, t.all_allowed_token, bucket, key)
	read, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("delete: url: %s: could not send final check request: %v", req.URL.String(), err.Error())
	}
	defer read.Body.Close()

	if read.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete: url: %s: we read something (status: %d) while it should be %d",
			req.URL.String(), read.StatusCode, http.StatusNotFound)
	}

	return nil
}

func test_bucket_bulk_delete(t *BackrunnerTest) error {
	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]

	keys := make([]string, 0)

	for i := 0; i < 1000; i++ {
		key := strconv.FormatInt(rand.Int63(), 16)
		keys = append(keys, key)

		// [1, 1+100) kbytes
		total_size := rand.Int31n(1000) + 100
		buf := make([]byte, total_size)
		body := bytes.NewReader(buf)
		req := t.NewRequest("POST", "upload", t.all_allowed_user, t.all_allowed_token, bucket, key, 0, 0, body)

		resp, err := t.client.Do(req)
		if err != nil {
			return fmt.Errorf("bulk-delete: url: %s: could not send upload request: %v", req.URL.String(), err.Error())
		}
		defer resp.Body.Close()

		_, err = t.check_upload_reply(bucket, key, resp)
		if err != nil {
			return err
		}
	}

	type bulk_delete struct {
		Keys	[]string	`json:"keys"`
	}

	bdel := bulk_delete {
		Keys: keys,
	}

	data, err := json.Marshal(&bdel)
	if err != nil {
		return fmt.Errorf("bulk-delete: could not marshal json: %v", err.Error())
	}

	req := t.NewRequest("POST", "bulk_delete", t.all_allowed_user, t.all_allowed_token, bucket, "", 0, 0, bytes.NewReader(data))
	dresp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("bulk-delete: url: %s: could not send bulk delete request: %v", req.URL.String(), err.Error())
	}
	defer dresp.Body.Close()

	data, err = ioutil.ReadAll(dresp.Body)
	if err != nil {
		return fmt.Errorf("bulk-delete url: %s: could not read response body: %v", req.URL.String(), err.Error())
	}

	if dresp.StatusCode != http.StatusOK {
		return fmt.Errorf("bulk-delete: url: %s: wrong status code: %d, must be %d, received data: %s",
			req.URL.String(), dresp.StatusCode, http.StatusOK, string(data))
	}

	for _, key := range keys {
		req = t.NewEmptyRequest("GET", "get", t.all_allowed_user, t.all_allowed_token, bucket, key)
		read, err := t.client.Do(req)
		if err != nil {
			return fmt.Errorf("bulk-delete: url: %s: could not send final check request: %v", req.URL.String(), err.Error())
		}
		defer read.Body.Close()

		if read.StatusCode != http.StatusNotFound {
			return fmt.Errorf("url: %s: we read something (status: %d) while it should be %d",
				req.URL.String(), read.StatusCode, http.StatusNotFound)
		}
	}

	return nil
}

func test_nobucket_upload(t *BackrunnerTest) error {
	key := strconv.FormatInt(rand.Int63(), 16)

	// [1, 1+100) kbytes
	total_size := 1024 * (rand.Int31n(100) + 1)
	buf := make([]byte, total_size)
	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "nobucket_upload", t.all_allowed_user, t.all_allowed_token, "", key, 0, 0, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = t.check_upload_reply("", key, resp)
	return err
}

func test_uniform_free_space(t *BackrunnerTest) error {
	the_last_chance := true
	for {
		key := strconv.FormatInt(rand.Int63(), 16)

		// [1024, 1024 * 1024)
		total_size := 1024 + rand.Int31n(1 * 1024 * 1024)
		buf := make([]byte, total_size)

		body := bytes.NewReader(buf)
		req := t.NewRequest("POST", "nobucket_upload", t.all_allowed_user, t.all_allowed_token, "", key, 0, 0, body)

		resp, err := t.client.Do(req)
		if err != nil {
			return fmt.Errorf("could not send upload request: %v", err)
		}
		defer resp.Body.Close()

		resp_data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		time.Sleep(time.Millisecond * 500)

		if resp.StatusCode != http.StatusOK {
			log.Printf("status: '%s', url: '%s', headers: req: %v, resp: %v, data-received: %s",
				resp.Status, resp.Request.URL.String(), resp.Request.Header, resp.Header, string(resp_data))

			if the_last_chance {
				the_last_chance = false
				time.Sleep(time.Second * 35)
				continue
			}

			break
		}

		// this 'last-chance' logic is simple - give it some time to run defragmentation
		// if we can write data into the storage - allow another last chance
		the_last_chance = true
	}

	// there should be no free space in any IO bucket, let's check it

	// sleep for bucket statistics to settle from previous tests, it is periodic
	time.Sleep(3 * time.Second)

	st, err := t.parse_stat()
	if err != nil {
		return err
	}

	for _, bname := range t.io_buckets {
		bucket, ok := st.Buckets[bname]
		if !ok {
			return fmt.Errorf("there is no IO bucket '%s' in the statistics, check out logs\n", bname)
		}

		for group_id, group := range bucket.Group {
			for _, ab := range group.Backends {
				sb := ab.Stat
				rate := float64(sb.VFS.TotalSizeLimit - sb.VFS.BackendUsedSize) / float64(sb.VFS.TotalSizeLimit)

				if rate >= t.free_space_ratio_soft {
					return fmt.Errorf("bucket: %s, group: %s, backend: %d, free-space-rate: %f, soft-limit: %f",
						bname, group_id, ab.Backend, rate, t.free_space_ratio_soft)
				}

				// this is invalid ugly check
				// it should not be allowed to hit hard limit,
				// but since proxy updates statistics once per some timeout,
				// it is possible that multiple files will be written during that timeout,
				// which will force rate to be less than hard limit when statistics has been
				// updated again
				if rate < t.free_space_ratio_hard / 2 {
					return fmt.Errorf("bucket: %s, group: %s, backend: %d, free-space-rate: %f, hard-limit: %f",
						bname, group_id, ab.Backend, rate, t.free_space_ratio_hard)
				}
			}
		}
	}

	return nil
}

func test_bucket_update(t *BackrunnerTest) error {
	bname := strconv.FormatInt(rand.Int63(), 16)
	user := strconv.FormatInt(rand.Int63(), 16)
	token := strconv.FormatInt(rand.Int63(), 16)
	key := "bucket-update-test"

	meta := bucket.BucketMsgpack {
		Version: 1,
		Name: bname,
		Groups: t.groups,
		Acl: make(map[string]bucket.BucketACL),
	}
	acl := bucket.BucketACL {
		Version: 1,
		User: user,
		Token: token,
		Flags: bucket.BucketAuthWrite,
	}
	meta.Acl[acl.User] = acl

	_, err := bucket.WriteBucket(t.ell, &meta)
	if err != nil {
		log.Fatalf("Could not upload bucket: %v", err)
	}

	// bucket has been uploaded into the storage,
	// let's check that reading/writing from that bucket succeeds

	err = t.upload_get_helper(bname, key, user, token)
	if err != nil {
		return err
	}

	// update ACL
	new_token := strconv.FormatInt(rand.Int63(), 16)
	acl = bucket.BucketACL {
		Version: 1,
		User: user,
		Token: new_token,
		Flags: bucket.BucketAuthWrite,
	}
	meta.Acl[acl.User] = acl

	_, err = bucket.WriteBucket(t.ell, &meta)
	if err != nil {
		log.Fatalf("Could not upload bucket: %v", err)
	}

	// trying to read data using old token, it should fail with 403 error
	req := t.NewEmptyRequest("GET", "get", user, new_token, bname, key)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("test_bucket_update: url: %s: could not send get request: %v", req.URL.String(), err)
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("test_bucket_update: url: %s: could not read reply: %v", req.URL.String(), req.Header, err)
	}

	if resp.StatusCode != http.StatusForbidden {
		return fmt.Errorf("test_bucket_update: url: %s, returned status: %d, must be: %d",
			req.URL.String(), resp.StatusCode, http.StatusForbidden)
	}

	// wait for ACL to update, it should be updated once per 30 seconds or so
	time.Sleep(32 * time.Second)

	// trying to update key using new token
	err = t.upload_get_helper(bname, "some another key", user, new_token)
	if err != nil {
		return err
	}

	return nil
}

type StatAB struct {
	Address		string
	Backend		int32
	Stat		*elliptics.StatBackend

}
type GroupStat struct {
	Backends		[]StatAB
	RecordsTotal		uint64
	RecordsRemoved		uint64
	RecordsCorrupted	uint64
}
type BStat struct {
	Group		map[string]GroupStat
	Meta		bucket.BucketMsgpack
}
type Stat struct {
	Buckets		map[string]BStat
}

func (t *BackrunnerTest) parse_stat() (*Stat, error) {
	req := t.NewEmptyRequest("GET", "stat/", t.all_allowed_user, t.all_allowed_token, "", "")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("parse_stats: url: %s: could not send get request: %v", req.URL.String(), err)
	}
	defer resp.Body.Close()

	stat_data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse_stats: url: %s: could not read reply: %v", req.URL.String(), req.Header, err)
	}

	var st Stat
	err = json.Unmarshal(stat_data, &st)
	if err != nil {
		return nil, fmt.Errorf("parse_stats: could not parse json statistics '%s': %v",
			string(stat_data), err)
	}

	log.Printf("stat: '%s'\n", string(stat_data))
	ioutil.WriteFile(fmt.Sprintf("%s/last_stat.json", t.base), stat_data, 0644)

	return &st, nil
}

func (st *Stat) total_operations(bname, command string) uint64 {
	var operations uint64 = 0

	bucket, ok := st.Buckets[bname]
	if ok {
		for _, group := range bucket.Group {
			for _, ab := range group.Backends {
				cmd, ok := ab.Stat.Commands[command]
				if ok {
					operations += cmd.RequestsSuccess
				}
			}
		}
	}

	return operations
}

func test_stats_update(t *BackrunnerTest) error {
	// sleep for bucket statistics to settle from previous tests, it is periodic
	time.Sleep(6 * time.Second)

	st1, err := t.parse_stat()
	if err != nil {
		return err
	}

	num := rand.Intn(1000) + 1000

	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("some key %d", i)
		err = t.upload_get_helper(bucket, key, t.all_allowed_user, t.all_allowed_token)
		if err != nil {
			return err
		}
	}

	// sleep for bucket statistics to update current operations, it is periodic
	time.Sleep(6 * time.Second)

	st2, err := t.parse_stat()
	if err != nil {
		return err
	}

	cmd := "WRITE"
	st2_num := st2.total_operations(bucket, cmd)
	st1_num := st1.total_operations(bucket, cmd)
	diff := st2_num - st1_num

	if diff != uint64(num) {
		return fmt.Errorf("operation counter differs: diff: %d (%d - %d), must be: %d",
			diff, st2_num, st1_num, num)
	}

	return nil
}

func test_bucket_file_update(t *BackrunnerTest) error {
	bname := strconv.FormatInt(rand.Int63(), 16)

	meta := bucket.BucketMsgpack {
		Version: 1,
		Name: bname,
		Groups: []uint32{t.groups[0], 12345},
		Acl: make(map[string]bucket.BucketACL),
	}
	acl := bucket.BucketACL {
		Version: 1,
		User: t.all_allowed_user,
		Token: t.all_allowed_token,
		Flags: bucket.BucketAuthWrite,
	}
	meta.Acl[acl.User] = acl

	_, err := bucket.WriteBucket(t.ell, &meta)
	if err != nil {
		log.Fatalf("Could not upload bucket: %v", err)
	}

	bfile, err := os.OpenFile(t.bucket_file, os.O_RDWR | os.O_APPEND, 0660)
	if err != nil {
		return fmt.Errorf("Could not open bucket file '%s': %v", t.bucket_file, err)
	}

	fmt.Fprintf(bfile, "%s\n", bname)
	bfile.Close()

	t.proxy_cmd.Process.Signal(syscall.SIGHUP)

	// wait for statistics to update
	time.Sleep(6 * time.Second)

	st, err := t.parse_stat()
	if err != nil {
		return err
	}

	_, ok := st.Buckets[bname]
	if !ok {
		return fmt.Errorf("There is no bucket '%s' in new stats", bname)
	}

	t.failed_bucket = bname
	return nil
}

// if there is no group it must be indicated in error groups
func test_write_failed(t *BackrunnerTest) error {
	if len(t.failed_bucket) == 0 {
		return fmt.Errorf("'failed bucket' was not setup in the test structure")
	}
	key := strconv.FormatInt(rand.Int63(), 16)

	total_size := 1024 * (rand.Int31n(100) + 1)
	buf := make([]byte, total_size)
	_, err := cryptorand.Read(buf)
	if err != nil {
		return fmt.Errorf("could not read random data: %v", err)
	}

	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "upload", t.all_allowed_user, t.all_allowed_token, t.failed_bucket, key, 0, 0, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not send upload request: %v", err)
	}
	defer resp.Body.Close()

	rep, err := t.check_upload_reply(t.failed_bucket, key, resp)
	if err != nil {
		return err
	}

	if len(rep.Reply.ErrorGroups) != 1 {
		return fmt.Errorf("failed write it not present in reply: %v", rep)
	}

	return nil
}

func (t *BackrunnerTest) gather_write_stats(num int) (cnt_smallest, cnt_biggest int, err error) {
	buf := make([]byte, 100)

	cryptorand.Read(buf)

	cnt_smallest = 100000000
	cnt_biggest = 0

	counters := make(map[string]int)
	for i := 0; i < num; i++ {
		key := strconv.FormatInt(rand.Int63(), 16)

		body := bytes.NewReader(buf)
		req := t.NewRequest("POST", "nobucket_upload", t.all_allowed_user, t.all_allowed_token, "", key, 0, 0, body)

		var resp *http.Response
		resp, err = t.client.Do(req)
		if err != nil {
			err = fmt.Errorf("gather-write-stats: could not send upload request: %v", err)
			return
		}
		defer resp.Body.Close()

		var rep *reply.Upload
		rep, err = t.check_upload_reply("", key, resp)
		if err != nil {
			err = fmt.Errorf("gather-write-stats: could not send upload request: %v", err)
			return
		}

		// only update stats for IO buckets
		// because of probabilistic nature of bucket selection,
		// it is possible to get ACL bucket, which has all @t.groups and its
		// total pain will *always* be equal to the pain of those groups
		//
		// IO buckets are test buckets which are bound to 1 group each
		// thus its pain will always be less than that of ACL bucket,
		// but it still can be selected
		//
		// since we slow down one backend in one group, let's be fair and
		// compare buckets only with 1 group/backend thus excluding ACL bucket
		// and anything else
		for _, bname := range t.io_buckets {
			if rep.Bucket == bname {
				counters[rep.Bucket]++
				break
			}
		}

		time.Sleep(300 * time.Millisecond)
	}

	for bname, cnt := range counters {
		if cnt < cnt_smallest {
			cnt_smallest = cnt
		}

		if cnt > cnt_biggest {
			cnt_biggest = cnt
		}

		log.Printf("gather-write-stats: bucket %s, writes: %d\n", bname, cnt)
	}

	return
}

func test_backend_slowdown(t *BackrunnerTest) error {
	num := 1000
	cnt_smallest, cnt_biggest, err := t.gather_write_stats(num)
	if err != nil {
		return err
	}

	// this should be roughly the same number of write hits per bucket,
	// since no bucket has been slown down
	if float64(cnt_biggest) / float64(cnt_smallest) > 2 {
		return fmt.Errorf("invalid distribution (should be rougly the same, i.e. difference < 0.5) in the equal run test: smallest: %d, biggest: %d",
		cnt_smallest, cnt_biggest)
	}

	s, err := elliptics.NewSession(t.ell.Node)
	if err != nil {
		return fmt.Errorf("could not create elliptics session: %v", err)
	}

	addr, err := elliptics.NewDnetAddrStr(t.elliptics_address[0])
	if err != nil {
		return fmt.Errorf("could not create address from '%s': %v", t.elliptics_address[0], err)
	}

	var backend_id int32 = 1
	var delay uint32 = 100
	for _ = range s.BackendSetDelay(&addr, backend_id, delay) {
	}

	time.Sleep(2 * time.Second)

	cnt_smallest, cnt_biggest, err = t.gather_write_stats(num)
	if err != nil {
		return err
	}

	if cnt_biggest / cnt_smallest < 3 {
		return fmt.Errorf("invalid distribution (diff must be more than several times, 3 in the code) in the slow run test: smallest: %d, biggest: %d",
		cnt_smallest, cnt_biggest)
	}

	delay = 0
	s.BackendSetDelay(&addr, backend_id, delay)

	time.Sleep(2 * time.Second)

	cnt_smallest, cnt_biggest, err = t.gather_write_stats(num)
	if err != nil {
		return err
	}

	// this should be roughly the same number of write hits per bucket,
	// since no bucket has been slown down
	if float64(cnt_biggest) / float64(cnt_smallest) > 2 {
		return fmt.Errorf("invalid distribution (should be rougly the same, i.e. difference < 0.5) in the SECOND equal run test: smallest: %d, biggest: %d",
		cnt_smallest, cnt_biggest)
	}


	return nil
}

var tests = [](func(t *BackrunnerTest) error) {
	TestBackendStatusUpdate,
	test_common_read,
	test_bucket_file_update,
	test_backend_slowdown,
	test_nobucket_upload,
	test_small_bucket_upload,
	test_big_bucket_upload,
	test_acl,
	test_stats_update,
	test_bucket_delete,
	test_bucket_bulk_delete,
	test_bucket_update,
	test_write_failed,
	test_uniform_free_space,
}

func FunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func TestResult(err error) string {
	if err == nil {
		return fmt.Sprintf("success")
	}

	return fmt.Sprintf(err.Error())
}

func update_log(file, msg string) {
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_APPEND, 0644)
	if err == nil {
		fmt.Fprintf(fd, "\n%s\n", msg)
	}
	fd.Close()
}

func Start(base, proxy_path string) {

	bt := &BackrunnerTest {
		base: base,
		server_log: fmt.Sprintf("%s/server.log", base),
		test_log: fmt.Sprintf("%s/backrunner.log", base),
		proxy_log: fmt.Sprintf("%s/proxy.log", base),
		client: &http.Client{},
		ell: nil,
		groups: []uint32{1,2,3},
		acl_bucket: strconv.FormatInt(rand.Int63(), 16),
		acl_key: strconv.FormatInt(rand.Int63(), 16),
		acl_buffer: make([]byte, 1024),
		acl_requests: make([]*CheckRequest, 0),
		all_allowed_user: "all-allowed-user",
		all_allowed_token: "all-allowed-token",
		io_buckets: make([]string, 0),
		free_space_ratio_hard: 0.15,
		free_space_ratio_soft: 0.2,
	}

	rand.Seed(3)

	defer func() {
		bt.server_cmd.Process.Signal(os.Interrupt)
		bt.proxy_cmd.Process.Signal(os.Interrupt)

		err := bt.server_cmd.Wait()
		log.Printf("dnet_ioserv process exited: %v", err)

		err = bt.proxy_cmd.Wait()
		log.Printf("proxy process exited: %v", err)
	}()

	bt.StartEllipticsServer()

	// create config after server has been started, since @elliptics_address array is filled
	// from the server's config address
	bt.conf = &config.ProxyConfig {
		Elliptics: config.EllipticsClientConfig {
			LogLevel: "notice",
			Remote: bt.elliptics_address,
			MetadataGroups: bt.groups,
		},

		Proxy: config.ProxyClientConfig {
			Address: fmt.Sprintf("localhost:%d", rand.Int31n(5000) + 60000),
			IdleTimeout: 60,
			FreeSpaceRatioSoft: bt.free_space_ratio_soft,
			FreeSpaceRatioHard: bt.free_space_ratio_hard,
			BucketUpdateInterval: 20,
			BucketStatUpdateInterval: 1,
			DefragFreeSpaceLimit: 0.4,
			DefragRemovedSpaceLimit: 0.1,
			DefragMaxBuckets: 2,
			DefragMaxBackendsPerServer: 2,
			Root: bt.base,
		},
	}

	bt.StartEllipticsClientProxy(proxy_path)

	for _, t := range tests {
		msg := fmt.Sprintf("TEST-STARTED: %s", FunctionName(t))
		update_log(bt.proxy_log, msg)
		update_log(bt.server_log, msg)
		update_log(bt.test_log, msg)

		err := t(bt)

		msg = fmt.Sprintf("TEST-COMPLETED: %s: %s", FunctionName(t), TestResult(err))
		update_log(bt.proxy_log, msg)
		update_log(bt.server_log, msg)
		update_log(bt.test_log, msg)

		fmt.Printf("%s: %s\n", FunctionName(t), TestResult(err))

		if err != nil {
			break
		}
	}
}
