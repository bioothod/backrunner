package main

import (
	"flag"
	"encoding/json"
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"io/ioutil"
	"log"
)

func bmeta_read_upload(ell *etransport.Elliptics, file string) (err error) {
	metaf, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Could not read upload bucket config file %s: %v", file, err)
	}

	var iface interface{}
	err = json.Unmarshal(metaf, &iface)
	if err != nil {
		err = fmt.Errorf("could not parse data: %v", err)
		return
	}

	generic := bucket.NewBucketMsgpack("generic")

	imap := iface.(map[string]interface{})

	if g, ok := imap["generic"]; ok {
		generic.ExtractJson(g)
	}

	if biface, ok := imap["buckets"]; ok {
		bmap := biface.(map[string]interface{})
		for bname, iface := range bmap {
			log.Printf("bucket: %s, iface: %p\n", bname, iface)

			tmp := bucket.NewBucketMsgpack(bname)
			*tmp = *generic
			tmp.Name = bname

			tmp.ExtractJson(iface)

			b, err := bucket.WriteBucket(ell, tmp)
			if err != nil {
				log.Printf("Could not write bucket %s: %v", bname, err)
			} else {
				log.Printf("%s\n", b.Meta.String())
			}
		}
	} else {
		err = fmt.Errorf("There is no 'buckets' section in metadata file, nothing to upload")
	}

	return
}

func main() {
	bname := flag.String("bucket", "", "bucket name to read")
	config_file := flag.String("config", "", "transport config file")
	upload := flag.String("upload", "", "bucket json file to upload/rewrite")
	flag.Parse()

	if *bname == "" && *upload == "" {
		log.Fatal("You must specify either bucket name to read of upload file with buckets")
	}

	if *config_file == "" {
		log.Fatal("You must specify Elliptics config file")
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
		err = bmeta_read_upload(ell, *upload)
		if err != nil {
			log.Fatalf("Could not write some buckets: %v", err)
		}
	} else {
		b, err = bucket.ReadBucket(ell, *bname)
		if err != nil {
			log.Fatalf("Could not read bucket %s: %v", *bname, err)
		}

		log.Printf("%s\n", b.Meta.String())
	}
}
