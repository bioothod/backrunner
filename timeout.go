package main

import (
	"errors"
	"net"
	"time"
)

type TimeoutConn struct {
	net.Conn
	readTimeout, writeTimeout time.Duration
}

var invalidOperationError = errors.New("TimeoutConn does not support or allow .SetDeadline operations")

func NewTimeoutConn(conn net.Conn, ioTimeout time.Duration) (*TimeoutConn, error) {
	return NewTimeoutConnReadWriteTO(conn, ioTimeout, ioTimeout)
}

func NewTimeoutConnReadWriteTO(conn net.Conn, readTimeout, writeTimeout time.Duration) (*TimeoutConn, error) {
	this := &TimeoutConn{
		Conn:         conn,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
	now := time.Now()
	err := this.Conn.SetReadDeadline(now.Add(this.readTimeout))
	if err != nil {
		return nil, err
	}
	err = this.Conn.SetWriteDeadline(now.Add(this.writeTimeout))
	if err != nil {
		return nil, err
	}
	return this, nil
}

func NewTimeoutConnDial(network, addr string, ioTimeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, ioTimeout)
	if err != nil {
		return nil, err
	}
	if conn, err = NewTimeoutConn(conn, ioTimeout); err != nil {
		return nil, err
	}
	return conn, nil
}

func (this *TimeoutConn) Read(data []byte) (int, error) {
	this.Conn.SetReadDeadline(time.Now().Add(this.readTimeout))
	return this.Conn.Read(data)
}

func (this *TimeoutConn) Write(data []byte) (int, error) {
	this.Conn.SetWriteDeadline(time.Now().Add(this.writeTimeout))
	return this.Conn.Write(data)
}

func (this *TimeoutConn) SetDeadline(time time.Time) error {
	return invalidOperationError
}

func (this *TimeoutConn) SetReadDeadline(time time.Time) error {
	return invalidOperationError
}

func (this *TimeoutConn) SetWriteDeadline(time time.Time) error {
	return invalidOperationError
}
