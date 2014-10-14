package btest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/etransport"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func (bt *BackrunnerTest) StartElliptics() {
	type Formatter struct {
		Type string
		Pattern string
	}
	type Rotation struct {
		Move int
	}
	type Sink struct {
		Type string
		Path string
		AutoFlush bool
		Rotation Rotation
	}
	type Frontend struct {
		Formatter Formatter
		Sink Sink
	}
	type Logger struct {
		Frontends []Frontend
		Level string
	}
	type Monitor struct {
		Port int32
	}
	type Options struct {
		Join bool
		Flags uint64
		Remote []string
		Address []string
		Wait_Timeout uint64
		Check_Timeout uint64
		NonBlocking_IO_Thread_Num int
		IO_Thread_Num int
		Net_Thread_Num int
		Daemon bool
		Auth_Cookie string
		Monitor Monitor
	}

	type Backend struct {
		Backend_ID uint32
		Type string
		Group uint32
		History string
		Data string
		Sync int
		Blob_Flags uint32
		Blob_Size string
		Records_In_Blob string
		Blob_Size_Limit string
	}
	type Config struct {
		Logger Logger
		Options Options
		Backends []Backend

	}

	config := Config {
		Logger: Logger {
			Frontends: []Frontend {
				Frontend {
					Formatter: Formatter {
						Type: "string",
						Pattern: "%(timestamp)s %(request_id)s/%(lwp)s/%(pid)s %(severity)s: %(message)s %(...L)s",
					},
					Sink: Sink {
						Type: "files",
						Path: fmt.Sprintf("%s/server.log", bt.base),
						AutoFlush: true,
						Rotation: Rotation {
							Move: 0,
							},
					},
				},
			},
			Level: "info",
		},
		Options: Options {
			Join: true,
			Flags: 20,
			Remote: []string {},
			Address: []string {
				fmt.Sprintf("localhost:%d:2-0", rand.Int31n(20000) + 20000),
			},
			Wait_Timeout: 60,
			Check_Timeout: 120,
			NonBlocking_IO_Thread_Num: 2,
			IO_Thread_Num: 2,
			Net_Thread_Num: 1,
			Daemon: false,
			Auth_Cookie: fmt.Sprintf("%016x", rand.Int63()),
			Monitor: Monitor {
				Port: rand.Int31n(20000) + 40000,
			},
		},
		Backends: make([]Backend, 0),
	}

	bt.elliptics_address = config.Options.Address

	var err error
	for i, g := range bt.groups {
		id := uint32(i + 1)
		backend := Backend {
			Backend_ID: id,
			Type: "blob",
			Group: g,
			History: fmt.Sprintf("%s/%d/history", bt.base, id),
			Data: fmt.Sprintf("%s/%d/data/data", bt.base, id),
			Sync: -1,
			Blob_Flags: 0, // bit 4 must not be set to enable blob-size-limit check
			Blob_Size: "20M",
			Records_In_Blob: "1000",
			Blob_Size_Limit: "40M",
		}

		err = os.MkdirAll(backend.History, 0755)
		if err != nil {
			log.Fatalf("Could not create directory '%s': %v", backend.History, err)
		}
		dir := filepath.Dir(backend.Data)
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			log.Fatalf("Could not create directory '%s': %v", dir, err)
		}

		config.Backends = append(config.Backends, backend)
	}

	data, err := json.Marshal(&config)
	if err != nil {
		log.Fatalf("Could not marshal elliptics config structure: %v", err)
	}

	file := fmt.Sprintf("%s/ioserv.conf", bt.base)
	err = ioutil.WriteFile(file, bytes.ToLower(data), 0644)
	if err != nil {
		log.Fatalf("Could not write file '%s': %v", file, err)
	}

	cmd := exec.Command("dnet_ioserv", "-c", file)
	err = cmd.Start()
	if err != nil {
		log.Fatalf("Could not start dnet_ioserv process: %v\n", err)
	}

	// wait 1 second for server to start
	time.Sleep(1 * time.Second)

	bt.server_cmd = cmd
}

func (bt *BackrunnerTest) Init(proxy_path string) {
	type ProxyConfig struct {
		LogFile string		`json:"log-file"`
		LogLevel string		`json:"log-level"`
		LogPrefix string	`json:"log-prefix"`
		Remote []string		`json:"remote"`
		MetadataGroups []uint32	`json:"metadata-groups"`
	}

	config := ProxyConfig {
		LogFile: fmt.Sprintf("%s/backrunner.log", bt.base),
		LogLevel: "debug",
		LogPrefix: "backrunner: ",
		Remote: bt.elliptics_address,
		MetadataGroups: bt.groups,
	}

	data, err := json.Marshal(&config)
	if err != nil {
		log.Fatalf("Could not marshal elliptics transport config structure: %v", err)
	}

	file := fmt.Sprintf("%s/elliptics_transport.conf", bt.base)
	err = ioutil.WriteFile(file, data, 0644)
	if err != nil {
		log.Fatalf("Could not write file '%s': %v", file, err)
	}

	bt.ell, err = etransport.NewEllipticsTransport(file)
	if err != nil {
		log.Fatal("Could not connect to elliptics server %v: %v", bt.elliptics_address, err)
	}

	bt.remote = fmt.Sprintf("localhost:%d", rand.Int31n(5000) + 60000)

	bt.bucket_file = fmt.Sprintf("%s/buckets", bt.base)
	fd, err := os.Create(bt.bucket_file)
	if err != nil {
		log.Fatalf("Could not create bucket file %s: %v\n", bt.bucket_file, err)
	}
	defer fd.Close()

	_, err = fmt.Fprintf(fd, "%s\n", bt.acl_bucket)
	if err != nil {
		log.Fatalf("Could not write acl bucket into bucket file: %v\n", err)
	}
	for _, g := range bt.groups {
		b := fmt.Sprintf("b%d", g)
		_, err = fmt.Fprintf(fd, "%s\n", b)
		if err != nil {
			log.Fatalf("Could not write bucket into bucket file: %v\n", err)
		}

		meta := bucket.BucketMsgpack {
			Version: 1,
			Name: b,
			Groups: []uint32{g},
			Acl: make(map[string]bucket.BucketACL),
		}

		acl := bucket.BucketACL {
			Version: 1,
			User: bt.all_allowed_user,
			Token: bt.all_allowed_token,
			Flags: bucket.BucketAuthWrite | bucket.BucketAuthNoToken,
		}
		meta.Acl[acl.User] = acl

		_, err := bucket.WriteBucket(bt.ell, &meta)
		if err != nil {
			log.Fatal("Could not upload bucket %s into storage: %v", b, err)
		}

		bt.io_buckets = append(bt.io_buckets, b)
	}

	bt.ACLInit()

	config = ProxyConfig {
		LogFile: fmt.Sprintf("%s/proxy.log", bt.base),
		LogLevel: "debug",
		LogPrefix: "proxy: ",
		Remote: bt.elliptics_address,
		MetadataGroups: bt.groups,
	}

	data, err = json.Marshal(&config)
	if err != nil {
		log.Fatalf("Could not marshal proxy config structure: %v", err)
	}

	file = fmt.Sprintf("%s/proxy.conf", bt.base)
	err = ioutil.WriteFile(file, data, 0644)
	if err != nil {
		log.Fatalf("Could not write file '%s': %v", file, err)
	}

	cmd := exec.Command(proxy_path, "-config", file, "-buckets", bt.bucket_file, "-listen", bt.remote)
	err = cmd.Start()
	if err != nil {
		log.Fatalf("Could not start proxy process: %v\n", err)
	}

	bt.proxy_cmd = cmd

	// wait 1 second for proxy to start
	time.Sleep(1 * time.Second)
}
