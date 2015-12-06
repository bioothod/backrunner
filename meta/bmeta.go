package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"io/ioutil"
	"log"
)

func bmeta_read_upload_file(ell *etransport.Elliptics, file, key string) (err error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Could not read file %s: %v", file, err)
	}

	ms, err := ell.MetadataSession()
	if err != nil {
		return
	}
	defer ms.Delete()

	ms.SetNamespace(bucket.BucketNamespace)

	tried := false

	for wr := range ms.WriteData(key, bytes.NewReader(data), 0, 0) {
		tried = true

		if wr.Error() != nil {
			err = wr.Error()

			log.Printf("%s: could not write file %s: %v", key, file, err)
			continue
		}

		fmt.Printf("Successfully uploaded %s into elliptics using key %s\n", file, key)
		return
	}

	if !tried {
		err = fmt.Errorf("%s: could not write file %s: WriteData() returned nothing", key, file)
	}
	return
}

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
				fmt.Printf("%s\n", b.Meta.String())
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
	backrunner_config := flag.String("upload-backrunner-config", "",
		"backrunner config file to upload into elliptics using 'backrunner-config-key' config parameter")
	bucket_list_upload := flag.String("upload-bucket-list", "",
		"bucket list to upload into elliptics using 'bucket-list-key' config parameter")
	upload := flag.String("upload", "", "bucket json file to upload/rewrite")
	flag.Parse()

	if *bname == "" && *upload == "" && *backrunner_config == "" && *bucket_list_upload == "" {
		log.Fatal("You must specify one (or more) of the following options:\n" +
				"* bucket name to read\n" +
				"* file with buckets metadata to upload\n" +
				"* list of buckets to upload\n" +
				"* backrunner config to upload\n")
	}

	if *config_file == "" {
		log.Fatal("You must specify Elliptics config file")
	}

	conf := &config.ProxyConfig{}
	err := conf.Load(*config_file)
	if err != nil {
		log.Fatalf("Could not load config file '%s': %v", *config_file, err)
	}

	ell, err := etransport.NewEllipticsTransport(conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	var b *bucket.Bucket

	if *upload != "" {
		err = bmeta_read_upload(ell, *upload)
		if err != nil {
			log.Fatalf("Could not write some buckets: %v", err)
		}
	}

	if *bname != "" {
		b, err = bucket.ReadBucket(ell, *bname)
		if err != nil {
			log.Fatalf("Could not read bucket %s: %v", *bname, err)
		}

		log.Printf("%s\n", b.Meta.String())
		fmt.Printf("%s\n", b.Meta.String())
	}

	if *backrunner_config != "" {
		if len(conf.Elliptics.BackrunnerConfig) == 0 {
			log.Fatalf("Requested uploading %s into elliptics as backrunner config, " +
				"but there is no 'backrunner-config-key' option",
				*backrunner_config)
		}

		err = bmeta_read_upload_file(ell, *backrunner_config, conf.Elliptics.BackrunnerConfig)
		if err != nil {
			log.Fatalf("upload: %v", err)
		}
	}

	if *bucket_list_upload != "" {
		if len(conf.Elliptics.BucketList) == 0 {
			log.Fatalf("Requested uploading %s into elliptics as list of buckets, " +
				"but there is no 'bucket-list-key' option",
				*bucket_list_upload)
		}

		err = bmeta_read_upload_file(ell, *bucket_list_upload, conf.Elliptics.BucketList)
		if err != nil {
			log.Fatalf("upload: %v", err)
		}
	}
}
