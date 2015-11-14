#!/usr/bin/env make

backrunner=github.com/bioothod/backrunner
backrunner_config=${backrunner}/config

BUILD_DATE=$(shell date "+%Y-%m-%d/%H:%M:%S/%z")

GO_LDFLAGS=-ldflags "-X ${backrunner_config}.BuildDate=${BUILD_DATE} \
	-X ${backrunner_config}.LastCommit=$(shell git rev-parse --short HEAD) \
	-X ${backrunner_config}.EllipticsGoLastCommit=$(shell GIT_DIR=${GOPATH}/src/github.com/bioothod/elliptics-go/.git git rev-parse --short HEAD)"

.DEFAULT: build
.PHONY: build

all: build

build:
	rm -f backrunner bmeta
	go build -o backrunner ${GO_LDFLAGS} proxy.go
	go build -o bmeta meta/bmeta.go

install: build
	cp -rf backrunner bmeta ${GOPATH}/bin/
