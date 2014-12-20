package btest

import (
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	cnf "github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func (bt *BackrunnerTest) StartEllipticsServer() {
	config := &cnf.EllipticsServerConfig {
		Logger: cnf.Logger {
			Frontends: []cnf.Frontend {
				cnf.Frontend {
					Formatter: cnf.Formatter {
						Type: "string",
						Pattern: "%(timestamp)s %(request_id)s/%(lwp)s/%(pid)s %(severity)s: %(message)s %(...L)s",
					},
					Sink: cnf.Sink {
						Type: "files",
						Path: bt.server_log,
						AutoFlush: true,
						Rotation: cnf.Rotation {
							Move: 0,
						},
					},
				},
			},
			Level: "notice",
		},
		Options: cnf.Options {
			Join: true,
			Flags: 20,
			Remote: []string {},
			Address: []string {
				fmt.Sprintf("localhost:%d:2-0", rand.Int31n(20000) + 20000),
			},
			Wait_Timeout: 60,
			Check_Timeout: 120,
			NonBlocking_IO_Thread_Num: 4,
			IO_Thread_Num: 4,
			Net_Thread_Num: 1,
			Daemon: false,
			Auth_Cookie: fmt.Sprintf("%016x", rand.Int63()),
			Monitor: cnf.Monitor {
				Port: rand.Int31n(20000) + 40000,
			},
		},
		Backends: make([]cnf.Backend, 0),
	}

	bt.elliptics_address = config.Options.Address

	var err error
	for i, g := range bt.groups {
		id := uint32(i + 1)
		backend := cnf.Backend {
			Backend_ID: id,
			Type: "blob",
			Group: g,
			History: fmt.Sprintf("%s/%d/history", bt.base, id),
			Data: fmt.Sprintf("%s/%d/data/data", bt.base, id),
			Sync: -1,
			Blob_Flags: 0, // bit 4 must not be set to enable blob-size-limit check
			Blob_Size: "20M",
			Records_In_Blob: 1000,
			Blob_Size_Limit: fmt.Sprintf("%dM", 200 + rand.Intn(5) * 20),
			PeriodicTimeout: 30,
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

	file := fmt.Sprintf("%s/ioserv.conf", bt.base)
	err = config.Save(file)
	if err != nil {
		log.Fatalf("Could not save config: %v", err)
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

func (bt *BackrunnerTest) StartEllipticsClientProxy(proxy_path string) {
	bt.conf.Elliptics.LogFile = bt.test_log
	bt.conf.Elliptics.LogPrefix = "backrunner: "

	file := fmt.Sprintf("%s/elliptics_transport.conf", bt.base)
	err := bt.conf.Save(file)
	if err != nil {
		log.Fatalf("Could not save client transport config: %v", err)
	}

	bt.ell, err = etransport.NewEllipticsTransport(bt.conf)
	if err != nil {
		log.Fatal("Could not connect to elliptics server %v: %v", bt.elliptics_address, err)
	}

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

	bt.conf.Elliptics.LogFile = bt.proxy_log
	bt.conf.Elliptics.LogPrefix = "proxy: "

	file = fmt.Sprintf("%s/proxy.conf", bt.base)
	err = bt.conf.Save(file)
	if err != nil {
		log.Fatalf("Could not save proxy config: %v", err)
	}

	cmd := exec.Command(proxy_path, "-config", file, "-buckets", bt.bucket_file)
	cmd.Stdout = &bt.proxy_stdout
	cmd.Stderr = &bt.proxy_stderr

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Could not start proxy process: %v\n", err)
	}

	bt.proxy_cmd = cmd

	// wait 1 second for proxy to start
	time.Sleep(1 * time.Second)
}
