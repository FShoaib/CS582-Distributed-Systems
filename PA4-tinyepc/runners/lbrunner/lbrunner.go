// DO NOT MODIFY THIS FILE!

package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"tinyepc/loadbalancer"
)

var (
	myport     = flag.Int("myport", 8000, "port this load balancer should listen to")
	ringweight = flag.Int("ringweight", 1, "weight of the hash ring")
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	flag.Parse()

	lb := loadbalancer.New(*ringweight)
	if lb == nil {
		log.Fatalln("New() not implemented")
	}

	err := lb.StartLB(*myport)
	if err != nil {
		log.Fatalln("Failed to start load balancer:", err)
	}
	defer lb.Close()
	log.Println("Started")

	reader := bufio.NewReader(os.Stdin)

	for {
		readString, _ := reader.ReadString('\n')
		if readString == "Close\n" {
			return
		}
	}
}
