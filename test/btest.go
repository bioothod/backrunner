package main

import (
	"flag"
	"github.com/bioothod/backrunner/test/btest"
	"log"
	"os"
)

func main() {
	base := flag.String("base", "", "Base directory to run tests in")
	proxy := flag.String("proxy", "", "Backrunner proxy binary path")
	flag.Parse()

	if *base == "" {
		log.Fatal("You must specify base directory")
	}

	if *proxy == "" {
		log.Fatal("You must specify backrunner proxy binary path")
	}

	err := os.RemoveAll(*base)
	if err != nil {
		log.Fatalf("Could not clean base directory '%s': %v\n", *base, err)
	}

	err = os.MkdirAll(*base, 0755)
	if err != nil {
		log.Fatalf("Could not create base directory '%s': %v\n", *base, err)
	}

	btest.Start(*base, *proxy)
}

