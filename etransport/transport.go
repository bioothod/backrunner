package etransport

import (
	"C"
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	"github.com/bioothod/backrunner/config"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
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

	s.SetTimeout(40)

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
			s.SetIOflags(elliptics.IOflag(val))
		}
	}
	cflags, ok := values["cflags"]
	if ok {
		val, err = strconv.ParseUint(cflags[0], 0, 64)
		if err == nil {
			s.SetCflags(elliptics.Cflag(val))
		}
	}
	trace, ok = values["trace_id"]
	if ok {
		trace_id, err = strconv.ParseUint(trace[0], 0, 64)
		if err != nil {
			trace_id = uint64(rand.Int63())
		}
	}

	s.SetTraceID(elliptics.TraceID(trace_id))

	return
}

func (e *Elliptics) Stat() (stat *elliptics.DnetStat, err error) {
	s, err := elliptics.NewSession(e.Node)
	if err != nil {
		return
	}
	defer s.Delete()

	stat = s.DnetStat()

	e.Lock()

	stat.Diff(e.prev_stat)
	e.prev_stat = stat

	e.Unlock()

	return
}

func NewEllipticsTransport(conf *config.ProxyConfig) (e *Elliptics, err error) {
	e = &Elliptics {
		prev_stat: nil,
	}

	if len(conf.Elliptics.LogFile) == 0 || len(conf.Elliptics.LogLevel) == 0 {
		log.Fatal("'log-file' and 'log-level' config parameters must be set")
	}

	e.LogFile, err = os.OpenFile(conf.Elliptics.LogFile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Could not open log file '%s': %q", conf.Elliptics.LogFile, err)
	}

	e.Log = log.New(e.LogFile, fmt.Sprintf("elliptics: %s", conf.Elliptics.LogPrefix), log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix(conf.Elliptics.LogPrefix)
	log.SetOutput(e.LogFile)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	e.Node, err = elliptics.NewNode(conf.Elliptics.LogFile, conf.Elliptics.LogLevel)
	if err != nil {
		log.Fatal(err)
	}


	if len(conf.Elliptics.Remote) == 0 {
		log.Fatal("'remote' config parameter must be set")
	}

	if len(conf.Elliptics.MetadataGroups) == 0 {
		log.Fatal("'metadata-groups' config parameter must be set")
	}

	e.MetadataGroups = conf.Elliptics.MetadataGroups

	err = e.Node.AddRemotes(conf.Elliptics.Remote)
	if err != nil {
		log.Fatalf("Could not connect to any remote node from %q: %q", conf.Elliptics.Remote, err)
	}

	return
}
