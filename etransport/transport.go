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
	"os"
	"unsafe"
)

type Elliptics struct {
	log_file	io.Writer
	log		*log.Logger

	node		*elliptics.Node
	metadata_groups	[]int32
}

func GoLogFunc(priv unsafe.Pointer, level int, msg *C.char) {
	e := (*Elliptics)(priv)
	e.log.Printf("%d: %s", level, C.GoString(msg))
}
var GoLogVar = GoLogFunc

func (e *Elliptics) MetadataSession() (ms *elliptics.Session, err error) {
	ms, err = elliptics.NewSession(e.node)
	if err != nil {
		return
	}

	ms.SetGroups(e.metadata_groups)
	return
}

func (e *Elliptics) DataSession() (s *elliptics.Session, err error) {
	s, err = elliptics.NewSession(e.node)
	if err != nil {
		return
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


	e.node, err = elliptics.NewNodeLog(unsafe.Pointer(&GoLogVar), unsafe.Pointer(e), int(conf["log-level"].(float64)))
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
