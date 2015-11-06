#!/usr/bin/env make

backrunner_config=github.com/bioothod/backrunner/config

GO_LDFLAGS=-ldflags "-X ${backrunner_config}.BuildDate=$(shell date -u +%Y-%m-%d/%H:%M:%S) \
	-X ${backrunner_config}.LastCommit=$(shell git rev-parse --short HEAD) \
	-X ${backrunner_config}.EllipticsGoLastCommit=$(shell GIT_DIR=${GOPATH}/src/github.com/bioothod/elliptics-go/.git git rev-parse --short HEAD)"

.DEFAULT: build
.PHONY: build

all: build

build:
	go build -o backrunner ${GO_LDFLAGS} proxy.go
