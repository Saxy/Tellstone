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
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/logger"
	"github.com/Saxy/Tellstone/server"
)

func initProfiling() {
	if _, ok := os.LookupEnv("TSD_ENABLE_PROFILING"); ok {
		runtime.SetMutexProfileFraction(100)
		runtime.SetBlockProfileRate(10000)
		go func() {
			_ = http.ListenAndServe("127.0.0.1:6060", nil)
		}()
	}
}

func initRuntimeSettings() {
	// TSD_GC_PERCENT configures the garbage collection trigger percentage.
	// Setting this to -1 disables the default percentage-based GC entirely.
	// This creates a "Zero-GC Hot-Path" where execution is never interrupted
	// by regular stop-the-world cycles during high-throughput bursts.
	gcPercent := -1
	if val, ok := os.LookupEnv("TSD_GC_PERCENT"); ok {
		if p, err := strconv.Atoi(val); err == nil {
			gcPercent = p
		}
	}
	debug.SetGCPercent(gcPercent)

	// TSD_MEM_LIMIT_BYTES acts as the global memory runtime ceiling.
	//
	// CRITICAL FOR DEPLOYMENT (Kubernetes/Containers):
	// Go treats this value as a soft target for the total heap size. When TSD_GC_PERCENT is -1,
	// the runtime will completely skip GC until the live heap approaches this threshold.
	// Once hit, a hard GC cycle is forced to prevent the application from expanding further.
	//
	// PRODUCTION BEST PRACTICE:
	// Never set this arbitrarily low. It should be configured to roughly 85-90% of the actual
	// container memory limit (e.g., set to 3.5GB if the K8s pod limit is 4GiB). This gives
	// TSD maximum memory runway for multi-gigabit throughput while leaving a 10-15% buffer
	// for internal network ring buffers (gnet) and kernel socket overhead, effectively
	// preventing predictable Linux OOM (SIGKILL) crashes.
	memLimit := int64(1024 * 1024 * 1024) // Default 1GB backup floor
	if val, ok := os.LookupEnv("TSD_MEM_LIMIT_BYTES"); ok {
		if m, err := strconv.ParseInt(val, 10, 64); err == nil {
			memLimit = m
		}
	}
	debug.SetMemoryLimit(memLimit)
}

func main() {
	initRuntimeSettings()
	initProfiling()
	cfg := config.LoadConfig(os.Args[1:])
	app := new(tellstone.App)
	app.Start(cfg, logger.NewSlogLogger(log.LevelError))
	svr := server.NewServer(app)
	svr.Run()
}
