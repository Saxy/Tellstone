/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example client that uses the binary protocol (OpCodes and response codes)
to interact with the Tellstone server.

Authors:

	Maximilian Hagen
*/
package main

import (
	"os"
	"time"

	"github.com/Saxy/Tellstone/client"
	"github.com/Saxy/Tellstone/logger"
)

func main() {
	slog := logger.NewSlogLogger(logger.LevelInfo)
	c, err := client.DialWithLogger("127.0.0.1:9988", 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer c.Close()

	// 4KB reusable scratch buffer for both building requests and receiving replies
	buf := make([]byte, 4*1024)

	res, err := c.Set([]byte("mykey"), []byte("myvalue"), 0, buf)
	if err != nil {
		fatal(slog, "SET failed", err)
	}
	slog.Log(logger.LevelInfo, "SET", logger.String("result", string(res)))

	res, err = c.Get([]byte("mykey"), buf)
	if err != nil {
		fatal(slog, "GET failed", err)
	}
	slog.Log(logger.LevelInfo, "GET", logger.String("result", string(res)))

	res, err = c.Delete([]byte("mykey"), buf)
	if err != nil {
		fatal(slog, "DELETE failed", err)
	}
	slog.Log(logger.LevelInfo, "DELETE", logger.String("result", string(res)))
}

// fatal logs the error and terminates the example process.
func fatal(l logger.Logger, msg string, err error) {
	if err != nil {
		l.Log(logger.LevelFatal, msg, logger.String("error", err.Error()))
	} else {
		l.Log(logger.LevelFatal, msg)
	}
	os.Exit(1)
}
