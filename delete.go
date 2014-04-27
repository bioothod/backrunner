package main

import (
	"flag"
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
	"strconv"
	"strings"
	"time"
)

func main() {
	remote := flag.String("remote", "", "remote address in the standard elliptics format: addr:port:family")
	logfile := flag.String("log", "/tmp/backrunner-delete-go.log", "elliptics log file")
	loglevel := flag.Int("log-level", int(elliptics.INFO), "elliptics log level")
	group_str := flag.String("metadata-groups", "11:12", "elliptics *metadata* groups")
	bucket := flag.String("bucket", "", "bucket name to process")
	flag.Parse()

	if *remote == "" {
		log.Fatal("You must provide remote address")
	}

	if *bucket == "" {
		log.Fatal("You must provide bucket name")
	}

	elog, err := elliptics.NewFileLogger(*logfile, *loglevel)
	if err != nil {
		log.Fatalln("NewFileLogger: ", err)
	}
	defer elog.Free()
	elog.Log(elliptics.INFO, "started: %v, logfile: %s, level: %d, remote: %s", time.Now(), *logfile, *loglevel, *remote)

	// Create elliptics node
	node, err := elliptics.NewNode(elog)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Free()

	node.SetTimeouts(100, 1000)
	if err = node.AddRemote(*remote); err != nil {
		log.Printf("AddRemote: %v", err)
	}

	session, err := elliptics.NewSession(node)
	if err != nil {
		log.Fatal("NewSession: %v", err)
	}

	var groups []int32
	for _, group := range strings.Split(*group_str, ":") {
		g, err := strconv.Atoi(group)
		if err != nil {
			log.Fatal("Invalid element in group string: %s", group)
		}

		groups = append(groups, int32(g))
	}

	session.SetGroups(groups)
	session.SetNamespace(*bucket)

	log.Println("Find all")
	for res := range session.FindAllIndexes([]string{"delete"}) {
		log.Printf("%v", res.Data())
	}
}
