package main

import (
	"flag"
	cnf "github.com/bioothod/backrunner/config"
	"log"
	"strings"
)

func main() {
	config_file := flag.String("config", "", "base config file")
	output_file := flag.String("output", "/dev/stdout", "file to put updated config")

	address := flag.String("address", "", "comma-separated list of server addresses")
	remote := flag.String("remote", "", "comma-separated list of remote nodes")
	log_file := flag.String("log-file", "", "log file")
	log_level := flag.String("log-level", "info", "log level")

	monitor_port := flag.Int("monitor_port", 0, "monitor port")

	backend_id := flag.Int("backend_id", -1, "backend ID")
	data_file := flag.String("data", "", "backend data field")
	history_file := flag.String("history", "", "backend history field")
	group := flag.Uint("group", 0, "group this backend belongs to")
	sync := flag.Int("sync", -1, "sync time in seconds (0 - sync after each write)")
	blob_flags := flag.Uint64("blob_flags", 0, "blob flags")
	blob_size := flag.String("blob_size", "10M", "blob size")
	blob_size_limit := flag.String("blob_size_limit", "", "maximum total size of all blobs")
	backend_type := flag.String("type", "blob", "backend type")
	defrag_percentage := flag.Int("defrag_percentage", 10, "defrag percentage")

	flag.Parse()

	config := &cnf.EllipticsServerConfig {}

	if *config_file == "" {
		log.Fatalf("You must specify base config file")
	}

	err := config.Load(*config_file)
	if err != nil {
		log.Fatalf("Could not load base config file: %v", err)
	}

	if *address != "" {
		config.Options.Address = strings.Split(*address, ",")
	}

	if *remote != "" {
		config.Options.Remote = strings.Split(*remote, ",")
	}

	if *log_file != "" {
		for i := range config.Logger.Frontends {
			f := &config.Logger.Frontends[i]
			if f.Sink.Type == "files" {
				f.Sink.Path = *log_file
				break
			}
		}
	}

	if *log_level != "" {
		config.Logger.Level = *log_level
	}

	if *monitor_port != 0 {
		config.Options.Monitor.Port = int32(*monitor_port)
	}

	setup_backend := func(b *cnf.Backend) {
		if *data_file != "" {
			b.Data = *data_file
		}
		if *history_file != "" {
			b.History = *history_file
		}
		if *group != 0 {
			b.Group = uint32(*group)
		}
		b.Sync = *sync
		b.Blob_Flags = *blob_flags
		b.Blob_Size = *blob_size
		b.DefragPercentage = *defrag_percentage
		b.Type = *backend_type

		if *blob_size_limit != "" {
			b.Blob_Size_Limit = *blob_size_limit
		}

		return
	}

	if *backend_id != -1 {
		found := false
		for i := range config.Backends {
			b := &config.Backends[i]
			if b.Backend_ID == uint32(*backend_id) {
				found = true
				setup_backend(b)
				break
			}
		}

		if !found {
			b := &cnf.Backend {
				Backend_ID: uint32(*backend_id),
			}

			setup_backend(b)
			config.Backends = append(config.Backends, *b)
		}
	}

	err = config.Save(*output_file)
	if err != nil {
		log.Fatalf("Could not save config: %v", err)
	}

	return
}
