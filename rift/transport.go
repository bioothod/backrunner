package rift

import (
	"encoding/json"
	"fmt"
	"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/transport"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
)

type Rift struct {
	remote	[]string
	client	*http.Client
}

func (r *Rift) generate_url(key, bucket, operation string) string {
	return fmt.Sprintf("http://%s/%s/%s/%s", r.remote[0], operation, bucket, key)
}

func (r *Rift) Upload(req *transport.Request) (resp *transport.Response, err error) {
	resp = &transport.Response {
	}

	http_req := req.Http
	defer http_req.Body.Close()

	http_req.URL, err = url.Parse(r.generate_url(req.Key, req.Bucket.Name, "upload"))
	if err != nil {
		resp.Status = http_resp.StatusBadRequest
		err = errors.NewKeyError(http_req.URL.String(), http.StatusBadRequest,
				fmt.Sprintf("could not parse generated URL: %q", err))
		return
	}

	sign, err := req.Bctl.GenAuthHeader(req.User, http_req)
	if err != nil {
		resp.Status = http_resp.StatusBadRequest
		err = errors.NewKeyError(http_req.URL.String(), http.StatusBadRequest,
				fmt.Sprintf("could not generate signature: user: %s: %q",
					req.User, err))
		return
	}

	http_req.Header[auth.AuthHeaderStr] = []string{"riftv1 " + req.User + ":" + sign}

	http_req.RequestURI = ""
	http_resp, err := r.client.Do(http_req)
	if err != nil {
		resp.Status = http_resp.StatusServiceUnavailable
		err = errors.NewKeyError(http_req.URL.String(), http.StatusServiceUnavailable,
				fmt.Sprintf("post failed: %q", err))
		return
	}
	defer http_resp.Body.Close()

	resp.Status = http_resp.StatusCode
	resp.Data, err = ioutil.ReadAll(http_resp.Body)
	if err != nil {
		resp.Status = http_resp.StatusServiceUnavailable
		err = errors.NewKeyError(http_req.URL.String(), http.StatusServiceUnavailable,
				fmt.Sprintf("failed to read back response data: %q", err))
		return
	}

	err = json.Unmarshal(resp.Data, &resp.Json)
	if err != nil {
		resp.Json = nil

		log.Printf("url: %s: upload: can not unmarshall rift reply: '%s', error: %q\n",
			http_req.URL, resp.Data, err)
	}

	return
}

func NewRiftTransport(remote []string) (r *Rift, err error) {
	r = &Rift {
		remote: remote,
		client: &http.Client {
			Transport: &http.Transport {
				Proxy:			func (req *http.Request) (*url.URL, error) {
								// no proxy allowed
								return nil, nil
							},
				MaxIdleConnsPerHost:	1024,
				DisableKeepAlives:	false,
				DisableCompression:	false,
				Dial:			func (network, addr string) (net.Conn, error) {
								return NewTimeoutConnDial(network, addr, transport.DEFAULT_IDLE_TIMEOUT)
							},
			},
		},
	}

	err = nil
	return
}
