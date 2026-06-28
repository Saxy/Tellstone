/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example client that connects to the Tellstone server, sends a request, and prints the response.

Authors:

	Maximilian Hagen
*/
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Saxy/Tellstone/internal/network"
)

func main() {
	client, err := network.Dial("127.0.0.1:9988", 5*time.Second)
	if err != nil {
		log.Fatalf("failed to dial server: %v", err)
	}
	defer client.Close()
	buf := make([]byte, 4*1024)
	call := func(payload string) {
		var resp network.Message
		if err := client.Call(network.MsgRequest, []byte(payload), buf, &resp); err != nil {
			log.Fatalf("call %q failed: %v", payload, err)
		}
		fmt.Printf("%s => %s\n", payload, string(resp.Payload))
	}
	call("SET mykey myvalue")
	call("GET mykey")
	call("DEL mykey")
	call("GET mykey")
}
