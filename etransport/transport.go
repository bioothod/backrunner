package etransport

import (
	"C"
	//"encoding/json"
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	//"github.com/bioothod/backrunner/auth"
	"github.com/bioothod/backrunner/config"
	//"github.com/bioothod/backrunner/errors"
	//"github.com/vmihailenco/msgpack"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	//"unsafe"
)

type Elliptics struct {
	LogFile		io.Writer
	Log		*log.Logger

	Node		*elliptics.Node
	MetadataGroups	[]uint32

	sync.Mutex
	prev_stat	*elliptics.DnetStat
}

func (e *Elliptics) MetadataSession() (ms *elliptics.Session, err error) {
	ms, err = elliptics.NewSession(e.Node)
	if err != nil {
		return
	}

	ms.SetGroups(e.MetadataGroups)
	return
}

func (e *Elliptics) DataSession(req *http.Request) (s *elliptics.Session, err error) {
	s, err = elliptics.NewSession(e.Node)
	if err != nil {
		return
	}

	values := req.URL.Query()
	var val uint64

	var trace_id uint64
	trace, ok := req.Header["X-Request"]
	if !ok {
		trace_id = uint64(rand.Int63())
	} else {
		trace_id, err = strconv.ParseUint(trace[0], 0, 64)
		if err != nil {
			trace_id = uint64(rand.Int63())
		}
	}

	ioflags, ok := values["ioflags"]
	if ok {
		val, err = strconv.ParseUint(ioflags[0], 0, 32)
		if err == nil {
			s.SetIOflags(uint32(val))
		}
	}
	cflags, ok := values["cflags"]
	if ok {
		val, err = strconv.ParseUint(cflags[0], 0, 64)
		if err == nil {
			s.SetCflags(val)
		}
	}
	trace, ok = values["trace_id"]
	if ok {
		trace_id, err = strconv.ParseUint(trace[0], 0, 64)
		if err != nil {
			trace_id = uint64(rand.Int63())
		}
	}

	s.SetTraceID(trace_id)

	return
}

func (e *Elliptics) Stat() (stat *elliptics.DnetStat, err error) {
	// this is kind of cache - we do not update statistics more frequently than once per second
	if e.prev_stat != nil && time.Since(e.prev_stat.Time).Seconds() <= 1.0 {
		stat = e.prev_stat
		return
	}

	s, err := elliptics.NewSession(e.Node)
	if err != nil {
		return
	}

	stat = s.DnetStat()

	e.Lock()
	defer e.Unlock()

	// if someone changed @prev_stat in parallel
	if e.prev_stat != nil && time.Since(e.prev_stat.Time).Seconds() <= 1.0 {
		stat = e.prev_stat
		return
	}

	stat.Diff(e.prev_stat)
	e.prev_stat = stat

	return
}

func NewEllipticsTransport(config_file string) (e *Elliptics, err error) {
	e = &Elliptics {
		prev_stat: nil,
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

	e.LogFile, err = os.OpenFile(conf["log-file"].(string), os.O_RDWR | os.O_APPEND | os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Could not open log file '%s': %q", conf["log-file"].(string), err)
	}

	e.Log = log.New(e.LogFile, fmt.Sprintf("elliptics: %s", prefix), log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix(prefix)
	log.SetOutput(e.LogFile)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	e.Node, err = elliptics.NewNode(e.Log, conf["log-level"].(string))
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
		e.MetadataGroups = append(e.MetadataGroups, uint32(m.(float64)))
	}

	err = e.Node.AddRemotes(remotes)
	if err != nil {
		log.Fatalf("Could not connect to any remote node from %q: %q", remotes, err)
	}

	return
}
