package elliptics

import (
	"C"
	//"encoding/json"
	//"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	//"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/config"
	//"github.com/bioothod/backrunner/errors"
	"github.com/bioothod/backrunner/transport"
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
}

func GoLogFunc(priv unsafe.Pointer, level int, msg *C.char) {
	e := (*Elliptics)(priv)
	e.log.Printf("%d: %s", level, C.GoString(msg))
}
var GoLogVar = GoLogFunc

func (e *Elliptics) Upload(req *transport.Request) (resp *transport.Response, err error) {
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

	if conf["log-file"] == nil || conf["log-level"] == nil {
		log.Fatal("'log-file' and 'log-level' config parameters must be set")
	}

	if conf["remote"] == nil {
		log.Fatal("'remote' config parameter must be set")
	}

	prefix := ""
	if conf["log-prefix"] != nil {
		prefix = conf["log-prefix"].(string)
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

	var remotes []string
	for _, r := range conf["remote"].([]interface{}) {
		remotes = append(remotes, r.(string))
	}

	err = e.node.AddRemotes(remotes)
	if err != nil {
		log.Fatalf("Could not connect to any remote node from %q: %q", remotes, err)
	}

	return
}
