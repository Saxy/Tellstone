/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Server entry point. Loads configuration and starts the Tellstone application.

Authors:

	Maximilian Hagen
*/
package main

import (
	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/logger"
)

func main() {
	cfg := config.LoadConfig()
	a := new(tellstone.App)
	a.Start(cfg, logger.NewSlogLogger(log.LevelInfo))
}
