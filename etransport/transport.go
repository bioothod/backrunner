package etransport

import (
	"C"
	//"encoding/json"
	//"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	//"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/config"
	//"github.com/bioothod/backrunner/errors"
	//"github.com/vmihailenco/msgpack"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	//"unsafe"
)

type Elliptics struct {
	log_file	io.Writer
	log		*log.Logger

	node		*elliptics.Node
	metadata_groups	[]int32
}

func (e *Elliptics) MetadataSession() (ms *elliptics.Session, err error) {
	ms, err = elliptics.NewSession(e.node)
	if err != nil {
		return
	}

	ms.SetGroups(e.metadata_groups)
	return
}

func (e *Elliptics) DataSession(req *http.Request) (s *elliptics.Session, err error) {
	s, err = elliptics.NewSession(e.node)
	if err != nil {
		return
	}

	values := req.URL.Query()
	var val uint64

	ioflags, ok := values["ioflags"]
	if ok {
		val, err = strconv.ParseUint(ioflags[0], 0, 32)
		if err != nil {
			return
		}
		s.SetIOflags(uint32(val))
	}
	cflags, ok := values["cflags"]
	if ok {
		val, err = strconv.ParseUint(cflags[0], 0, 64)
		if err != nil {
			return
		}
		s.SetCflags(val)
	}
	trace, ok := values["trace_id"]
	if ok {
		val, err = strconv.ParseUint(trace[0], 0, 64)
		if err != nil {
			return
		}
		s.SetTraceID(val)
	}

	return
}

func NewEllipticsTransport(config_file string) (e *Elliptics, err error) {
	e = &Elliptics {
	}

	conf_interface, err := config.Parse(config_file)
	if err != nil {
		log.Fatalf("Could not parse config %s: %q", config_file, err)
	}
	conf := conf_interface.(map[string]interface{})

	prefix := ""
	if conf["log-prefix"] != nil {
		prefix = conf["log-prefix"].(string)
	}

	if conf["log-file"] == nil || conf["log-level"] == nil {
		log.Fatal("'log-file' and 'log-level' config parameters must be set")
	}

	e.log_file, err = os.OpenFile(conf["log-file"].(string), os.O_RDWR | os.O_APPEND | os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Could not open log file '%s': %q", conf["log-file"].(string), err)
	}

	e.log = log.New(e.log_file, prefix, log.LstdFlags | log.Lmicroseconds)

	e.node, err = elliptics.NewNode(e.log, conf["log-level"].(string))
	if err != nil {
		log.Fatal(err)
	}


	if conf["remote"] == nil {
		log.Fatal("'remote' config parameter must be set")
	}

	var remotes []string
	for _, r := range conf["remote"].([]interface{}) {
		remotes = append(remotes, r.(string))
	}


	if conf["metadata-groups"] == nil {
		log.Fatal("'metadata-groups' config parameter must be set")
	}

	for _, m := range conf["metadata-groups"].([]interface{}) {
		e.metadata_groups = append(e.metadata_groups, int32(m.(float64)))
	}

	err = e.node.AddRemotes(remotes)
	if err != nil {
		log.Fatalf("Could not connect to any remote node from %q: %q", remotes, err)
	}

	return
}
