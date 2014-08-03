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
	file	io.Writer
	node	*elliptics.Node

	log	*log.Logger
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

	e.file, err = os.OpenFile(conf["log-file"].(string), os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Could not open log file '%s': %q", conf["log-file"].(string), err)
	}

	e.log = log.New(e.file, "test-prefix:", log.LstdFlags | log.Lmicroseconds)
	e.log.Printf("test\n")

	e.node, err = elliptics.NewNodeLog(unsafe.Pointer(&GoLogVar), unsafe.Pointer(e), int(conf["log-level"].(float64)))
	if err != nil {
		log.Fatal(err)
	}
	defer e.node.Free()

	var remotes []string
	for _, r := range conf["remote"].([]interface{}) {
		remotes = append(remotes, r.(string))
	}

	return
}
