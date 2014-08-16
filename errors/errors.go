package errors

import (
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
	"syscall"
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

func ErrorStatus(err error) int {
	if ke, ok := err.(*KeyError); ok {
		return ke.status
	}

	return 555
}

func ErrorData(err error) string {
	if ke, ok := err.(*KeyError); ok {
		return ke.data
	}

	return err.Error()
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

func NewKeyErrorFromEllipticsError(ellerr error, url, message string) (err *KeyError) {
	err_code := elliptics.ErrorStatus(ellerr)
	err_message := elliptics.ErrorData(ellerr)
	status := http.StatusBadRequest

	switch syscall.Errno(-err_code) {
	case syscall.ENXIO:
		status = http.StatusServiceUnavailable
	case syscall.ENOENT:
		status = http.StatusNotFound
	}

	err = errors.NewKeyError(url, status,
		fmt.Sprintf("%s: elliptics-code: %d, elliptics-message: %s",
			message, err_code, err_message))
	return
}
