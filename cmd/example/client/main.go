/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example client that uses the binary protocol (OpCodes and response codes) to interact with the Tellstone server.

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

	// 4KB reusable scratch buffer for both building requests and receiving replies
	buf := make([]byte, 4*1024)

	// SET
	res, _ := client.Set([]byte("mykey"), []byte("myvalue"), 0, buf)
	fmt.Printf("SET => %s\n", string(res))

	// GET
	res, _ = client.Get([]byte("mykey"), buf)
	fmt.Printf("GET => %s\n", string(res))

	// DELETE
	res, _ = client.Delete([]byte("mykey"), buf)
	fmt.Printf("DEL => %s\n", string(res))
}
