package main

import (
	"flag"
	"log"
	"strconv"
)

var (
	lbPort = flag.Int("port", 8000, "Port of the LoadBalancer")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()

	lbAddr := ":" + strconv.Itoa(*lbPort)
	_ = lbAddr

	// TODO: Implement this!

	select {}
}
