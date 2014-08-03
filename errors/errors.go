package errors

import (
	"fmt"
	"log"
)

type KeyError struct {
	url	string
	status	int
	data	string
}

func (k *KeyError) Error() string {
	return fmt.Sprintf("url: %s: error code: %d, returned data: '%s'",
		k.url, k.status, fmt.Sprintf("%s", k.data))
}

func NewKeyError(url string, status int, data string) (err error) {
	err = &KeyError{
		url:    url,
		status: status,
		data:   data,
	}
	log.Printf("%s", err)
	return
}

