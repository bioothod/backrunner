package errors

import (
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	"net/http"
	"syscall"
)

type KeyError struct {
	url	string
	status	int
	data	string
}

func (k *KeyError) Error() string {
	return fmt.Sprintf("url: %s: error status: %d, returned data: '%s'",
		k.url, k.status, k.data)
}

func EllipticsErrorToStatus(err error) int {
	status := http.StatusBadRequest

	if de, ok := err.(*elliptics.DnetError); ok {
		err_code := elliptics.ErrorCode(de)

		switch syscall.Errno(-err_code) {
		case syscall.ENXIO:
			status = http.StatusServiceUnavailable
		case syscall.ETIMEDOUT:
			status = http.StatusServiceUnavailable
		case syscall.EIO:
			status = http.StatusServiceUnavailable
		case syscall.ENOENT:
			status = http.StatusNotFound
		case syscall.EILSEQ:
			status = http.StatusNotFound
		case syscall.EBADFD:
			status = http.StatusNotFound
		case syscall.EINVAL:
			status = http.StatusBadRequest
		}
	}

	return status
}

func ErrorStatus(err error) int {
	if ke, ok := err.(*KeyError); ok {
		return ke.status
	}

	return EllipticsErrorToStatus(err)
}

func ErrorData(err error) string {
	if ke, ok := err.(*KeyError); ok {
		return ke.data
	}
	if ke, ok := err.(*elliptics.DnetError); ok {
		return ErrorData(ke)
	}

	return err.Error()
}

func NewKeyError(url string, status int, data string) (err *KeyError) {
	err = &KeyError{
		url:    url,
		status: status,
		data:   data,
	}
	return
}

func NewKeyErrorFromEllipticsError(ellerr error, url, message string) (err *KeyError) {
	err_code := elliptics.ErrorCode(ellerr)
	err_message := elliptics.ErrorData(ellerr)
	status := EllipticsErrorToStatus(ellerr)

	err = NewKeyError(url, status,
		fmt.Sprintf("%s: elliptics-code: %d, elliptics-message: %s",
			message, err_code, err_message))
	return
}
