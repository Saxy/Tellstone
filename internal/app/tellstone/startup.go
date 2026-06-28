/*
Package tellstone
Tellstone Cloud-Native In-Memory Database
File: startup.go
Description: Application bootstrap that stores logger, config and prints a startup banner.

Authors:

	Maximilian Hagen
*/
package tellstone

import (
	"fmt"
	"strings"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
)

type App struct {
	logger log.Logger
	config *config.Config
}

func (a *App) Start(cfg *config.Config, logger log.Logger) {
	a.logger = logger
	a.config = cfg
	banner := `
  ______________    __   _______________  _   ________
 /_  __/ ____/ /   / /  / ___/_  __/ __ \/ | / / ____/
  / / / __/ / /   / /   \__ \ / / / / / /  |/ / __/   
 / / / /___/ /___/ /______/ // / / /_/ / /|  / /___   
/_/ /_____/_____/_____/____//_/  \____/_/ |_/_____/ 
		                       >> TSD CORE ENGINE <<
github: https://github.com/Saxy/Tellstone
`
	fmt.Println("\033[36m" + banner + "\033[0m")
	fmt.Println("\033[90m" + strings.Repeat("-", 70) + "\033[0m")
	logger.Log(log.LevelInfo, "TSD Core Engine initializing",
		log.String("bind_address", cfg.GetAddr()),
		log.String("max_msg_size", (new(config.ByteSize(cfg.GetMaxMsgSize()))).String()),
		log.Uint64("max_msg_size_bytes", cfg.GetMaxMsgSize()),
		log.String("evict_interval", cfg.GetEvictTicker().String()),
		log.Int("evict_slots", int(cfg.GetEvictSlots())),
		log.String("log_level", cfg.GetLogLevel().String()),
	)
	if cfg.EncryptionEnabled() {
		if cfg.GetEncryptionKey() == "" {
			logger.Log(log.LevelFatal, "Encryption key must be provided", log.String("error", "encryption key is missing but encryption is enabled"))
		}
		logger.Log(log.LevelInfo, "Engine crypto status", log.String("encryption", "ENABLED (ChaCha20-Poly1305)"))
	} else {
		logger.Log(log.LevelWarn, "Engine crypto status", log.String("encryption", "DISABLED (Plaintext Mode)"))
	}
	if cfg.GetTraceRatio() > 0 {
		logger.Log(log.LevelInfo, "Telemetry stack configuration",
			log.String("telemetry", "OTLP/gRPC Active"),
			log.Float("sample_ratio", cfg.GetTraceRatio()),
		)
	} else {
		logger.Log(log.LevelInfo, "Telemetry stack configuration", log.String("telemetry", "NoOp Tracer"))
	}
}

func (a *App) GetLogger() log.Logger     { return a.logger }
func (a *App) GetConfig() *config.Config { return a.config }
