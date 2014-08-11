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
		k.url, k.status, k.data)
}

func (k *KeyError) Status() int {
	return k.status
}

func NewKeyError(url string, status int, data string) (err *KeyError) {
	err = &KeyError{
		url:    url,
		status: status,
		data:   data,
	}
	log.Printf("%v\n", err.Error())
	return
}

