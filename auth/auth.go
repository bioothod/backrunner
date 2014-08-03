package auth

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"github.com/bioothod/backrunner/errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const AuthHeaderStr string = "Authorization"

func GetAuthInfo(r *http.Request) (user, recv_auth string, err error) {
	user = ""
	recv_auth = ""
	err = nil

	auth_headers, ok := r.Header[AuthHeaderStr]
	if !ok {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: there is no '%s' header\n",
				r.URL, AuthHeaderStr))
		return
	}

	auth_data := strings.Split(auth_headers[0], " ")
	if len(auth_data) != 2 {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: auth header1 '%s' must be 'riftv1 user:hmac'\n",
				r.URL, auth_headers[0]))
		return
	}

	auth_data = strings.Split(auth_data[1], ":")
	if len(auth_data) != 2 {
		err = errors.NewKeyError(r.URL.String(), http.StatusForbidden,
			fmt.Sprintf("url: %s: auth header2 '%s' must be 'riftv1 user:hmac'\n",
				r.URL, auth_headers[0]))
		return
	}

	user = auth_data[0]
	recv_auth = auth_data[1]
	return
}

func GenerateSignature(key, method string, u *url.URL, headers map[string][]string) (ret string, err error) {
	text := method + "\n"
	text += u.Path

	q := u.Query()
	if len(q) > 0 {
		var keys []string

		for k, _ := range q {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		sorted_query := url.Values{}
		for _, k := range keys {
			val := q[k][0]
			if len(val) > 0 {
				sorted_query.Set(k, val)
			}
		}

		text += "?" + sorted_query.Encode()
	}
	text += "\n"

	if len(headers) > 0 {
		var keys []string
		lower_headers := make(map[string]string)

		for k, v := range headers {
			lower := strings.ToLower(k)
			if strings.HasPrefix(lower, "x-ell-") {
				keys = append(keys, lower)
				lower_headers[lower] = v[0]
			}
		}
		sort.Strings(keys)

		for _, k := range keys {
			text += k + ":" + lower_headers[k] + "\n"
		}
	}

	mac := hmac.New(sha512.New, []byte(key))
	mac.Write([]byte(text))

	ret = hex.EncodeToString(mac.Sum(nil))
	return

	fmt.Printf("\"%s\"\n%s\n", text, ret)


	return ret, nil
}

func main1() {
	headers := make(map[string][]string)
	headers["QWE"] = []string{"qwe string"}
	headers["X-ell-ololo"] = []string{"trash", "secong header which is ignored"}
	u, err := url.Parse("http://storage.coub.com/get/bucket:12.21/test.txt?user=Mary&timestamp=12345&boolean")
	if err != nil {
		return
	}

	GenerateSignature("secure string", "GET", u, headers)
}
