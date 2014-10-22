package main

import (
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"io/ioutil"
	"log"
)

func main() {
	bname := flag.String("bucket", "", "bucket name to read or upload")
	config_file := flag.String("config", "", "transport config file")
	upload := flag.String("upload", "", "bucket json file to upload/rewrite")
	flag.Parse()

	if *bname == "" {
		log.Fatal("there is no bucket")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	cnf := &config.ProxyConfig{}
	err := cnf.Load(*config_file)
	if err != nil {
		log.Fatalf("Could not load config file '%s': %v", *config_file, err)
	}

	ell, err := etransport.NewEllipticsTransport(cnf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	var b *bucket.Bucket

	if *upload != "" {
		meta, err := ioutil.ReadFile(*upload)
		if err != nil {
			log.Fatalf("Could not read bucket file %s: %v", *upload, err)
		}
		b, err = bucket.WriteBucketJson(ell, *bname, meta)
		if err != nil {
			log.Fatalf("Could not write bucket %s: %v", *bname, err)
		}
	} else {
		b, err = bucket.ReadBucket(ell, *bname)
		if err != nil {
			log.Fatalf("Could not read bucket %s: %v", *bname, err)
		}
	}

	var acls []string
	for _, acl := range b.Meta.Acl {
		acls = append(acls, fmt.Sprintf("%s:%s:0x%x", acl.User, acl.Token, acl.Flags))
	}

	log.Printf("%s: version: %d, groups: %v, flags: 0x%x, max-size: %d, max-key-num: %d, acls: %v",
		b.Meta.Name, b.Meta.Version, b.Meta.Groups, b.Meta.Flags, b.Meta.MaxSize, b.Meta.MaxKeyNum, acls)
}
