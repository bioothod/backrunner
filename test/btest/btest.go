package btest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
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

	server_cmd *exec.Cmd
	proxy_cmd *exec.Cmd

	elliptics_address []string

	remote string
	client *http.Client

	ell *etransport.Elliptics


	// buffer used in ACL check requests, it is never freed
	acl_buffer []byte

	// test bucket with wide variety of ACLs
	acl_bucket string

	groups []int32

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
}

func (t *BackrunnerTest) check_upload_reply(bucket, key string, resp *http.Response) error {
	var err error

	resp_data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status: '%s', url: '%s', headers: req: %v, resp: %v, data-received: %s",
			resp.Status, resp.Request.URL.String(), resp.Request.Header, resp.Header, string(resp_data))
	}

	var rep reply.Upload
	err = json.Unmarshal(resp_data, &rep)
	if err != nil {
		return fmt.Errorf("invalid reply '%s': %s", string(resp_data), err.Error())
	}

	if rep.Primary.Key != key {
		return fmt.Errorf("invalid reply '%s': keys do not match: sent: %s, recv: %s", string(resp_data), key, rep.Primary.Key)
	}

	if rep.Bucket == "" {
		return fmt.Errorf("invalid reply '%s': returned invalid bucket name: '%s'", string(resp_data), rep.Bucket)
	}

	if bucket != "" {
		if rep.Bucket != bucket {
			return fmt.Errorf("invalid reply '%s': buckets do not match: sent: %s, recv: %s",
				string(resp_data), bucket, rep.Bucket)
		}
	}

	return nil
}

func (t *BackrunnerTest) NewRequest(method, handler, user, token, bucket, key string, body io.Reader) *http.Request {
	var url string
	if bucket == "" {
		url = fmt.Sprintf("http://%s/%s/%s", t.remote, handler, key)
	} else {
		url = fmt.Sprintf("http://%s/%s/%s/%s", t.remote, handler, bucket, key)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Fatal("Could not create request: method: %s, url: '%s': %v\n", method, url, err)
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

func (t *BackrunnerTest) NewCheckRequest(method, handler, user, token string, status int) *CheckRequest {
	body := bytes.NewReader(t.acl_buffer)

	ret := &CheckRequest {
		request: t.NewRequest(method, handler, user, token, t.acl_bucket, t.acl_key, body),
		status: status,
	}

	log.Printf("method: %s, handler: %s, user: %s, token: %s, url: %s, headers: %v\n",
		method, handler, user, token, ret.request.URL.String(), ret.request.Header)

	return ret
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
		log.Fatal("Could not upload bucket: %v", err)
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

func test_big_bucket_upload(t *BackrunnerTest) error {
	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]
	key := strconv.FormatInt(rand.Int63(), 16)

	// [20, 20+25) megabytes
	total_size := 1024 * (rand.Int31n(25 * 1024) + 20 * 1024)
	buf := make([]byte, total_size)
	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "upload", t.all_allowed_user, t.all_allowed_token, bucket, key, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return t.check_upload_reply(bucket, key, resp)
}

func test_small_bucket_upload(t *BackrunnerTest) error {
	bucket := t.io_buckets[rand.Intn(len(t.io_buckets))]
	key := strconv.FormatInt(rand.Int63(), 16)

	// [1, 1+100) kbytes
	total_size := 1024 * (rand.Int31n(100) + 1)
	buf := make([]byte, total_size)
	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "upload", t.all_allowed_user, t.all_allowed_token, bucket, key, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return t.check_upload_reply(bucket, key, resp)
}

func test_nobucket_upload(t *BackrunnerTest) error {
	key := strconv.FormatInt(rand.Int63(), 16)

	// [1, 1+100) kbytes
	total_size := 1024 * (rand.Int31n(100) + 1)
	buf := make([]byte, total_size)
	body := bytes.NewReader(buf)
	req := t.NewRequest("POST", "nobucket_upload", t.all_allowed_user, t.all_allowed_token, "", key, body)

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return t.check_upload_reply("", key, resp)
}


var tests = [](func(t *BackrunnerTest) error) {
	test_nobucket_upload,
	test_small_bucket_upload,
	test_big_bucket_upload,
	test_acl,
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

func Start(base, proxy_path string) {
	bt := &BackrunnerTest {
		base: base,
		remote:	"",
		client: &http.Client{},
		ell: nil,
		groups: []int32{1,2,3},
		acl_bucket: strconv.FormatInt(rand.Int63(), 16),
		acl_key: strconv.FormatInt(rand.Int63(), 16),
		acl_buffer: make([]byte, 1024),
		acl_requests: make([]*CheckRequest, 0),
		all_allowed_user: "all-allowed-user",
		all_allowed_token: "all-allowed-token",
		io_buckets: make([]string, 0),
	}

	rand.Seed(3)

	bt.StartElliptics()
	defer func() {
		bt.server_cmd.Process.Signal(os.Interrupt)
		bt.proxy_cmd.Process.Signal(os.Interrupt)

		err := bt.server_cmd.Wait()
		log.Printf("dnet_ioserv process exited: %v", err)

		err = bt.proxy_cmd.Wait()
		log.Printf("proxy process exited: %v", err)
	}()

	bt.Init(proxy_path)

	for _, t := range tests {
		err := t(bt)
		fmt.Printf("%s: %s\n", FunctionName(t), TestResult(err))

		if err != nil {
			break
		}
	}
}
