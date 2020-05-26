// DO NOT MODIFY THIS FILE!

package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strconv"
	"tinyepc/mme"
)

var (
	myPort = flag.Int("myport", 8001, "Port Number this mme should listen to")
	lbPort = flag.Int("lbport", 8000, "Port Number of Load Balancer")
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	flag.Parse()

	lbAddress := ":" + strconv.Itoa(*lbPort)

	mme := mme.New()
	if mme == nil {
		log.Fatalln("New() not implemented")
	}

	err := mme.StartMME(":"+strconv.Itoa(*myPort), lbAddress)
	if err != nil {
		log.Fatalln("Failed to start MME:", err)
	}
	defer mme.Close()
	log.Println("Started")

	reader := bufio.NewReader(os.Stdin)

	for {
		readString, _ := reader.ReadString('\n')
		if readString == "Close\n" {
			return
		}
	}
}
