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
	"github.com/Saxy/Tellstone/internal/version"
)

type App struct {
	logger log.Logger
	config *config.Config
}

func (a *App) Start(cfg *config.Config, logger log.Logger) {
	a.logger = logger
	a.config = cfg
	banner := `                        
               @@@@@@@               
            @@@%%%@@@@@@@            
         @@%%%%%%%=%@@@@@@@@         
      @@%%#*%%%%%%+@@@@@@*#@@@@      Tellstone
     @@@%%#++#%%%%+####*++%@@@@@     >> TSD CORE ENGINE <<     
     @@@@@@@*+*%%%=###++#@@@@@@@     https://github.com/Saxy/Tellstone  
     @@@@@@@###+=+*+=*%@@@@@@@@@     Version: ` + version.Version + `
     @@%####**#+@#+#@+%##%%%%@@@     SHA: ` + version.Commit + `
     @@%####*##+@#+#@+%**%%%%@@@     
     @@@@@@@@@%*=+*+=*@%%@@@@@@@     
     @@@@@@@#+*#%#=@@@#*#@@@@@@@     
     @@@@@%**#%%%%+@@@@@**@@@@@@     
      @@@@%#@@@@@@*@@@@@@#%@@@@      
         @@@@@@@@@=@@@@@@@@@         
            @@@@@@@@@@@@@            
               @@@@@@@
`
	fmt.Println("\033[36m" + banner + "\033[0m")
	fmt.Println("\033[90m" + strings.Repeat("-", 70) + "\033[0m")
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "TSD Core Engine initializing",
			log.String("version", version.Version),
			log.String("commit", version.Commit),
			log.String("bind_address", cfg.GetAddr()),
			log.String("max_msg_size", (new(config.ByteSize(cfg.GetMaxMsgSize()))).String()),
			log.Uint64("max_msg_size_bytes", cfg.GetMaxMsgSize()),
			log.String("evict_interval", cfg.GetEvictTicker().String()),
			log.Int("evict_slots", int(cfg.GetEvictSlots())),
			log.String("log_level", cfg.GetLogLevel().String()),
		)
	}
	if cfg.EncryptionEnabled() {
		// A missing key source is rejected in config.LoadConfig, which runs before
		// this banner; reaching this branch means a key is present.
		keySource := "flag/env"
		if cfg.GetEncryptionKeyFile() != "" {
			keySource = "file"
		}
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "Engine crypto status",
				log.String("encryption", "ENABLED (ChaCha20-Poly1305)"),
				log.String("key_source", keySource),
			)
		}
	} else {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "Engine crypto status", log.String("encryption", "DISABLED (Plaintext Mode)"))
		}
	}
	if cfg.GetTraceRatio() > 0 {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "Telemetry stack configuration",
				log.String("telemetry", "OTLP/gRPC Active"),
				log.Float("sample_ratio", cfg.GetTraceRatio()),
			)
		}
	} else {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "Telemetry stack configuration", log.String("telemetry", "NoOp Tracer"))
		}
	}
	// --rbac-config and --require-pass are both authentication knobs; when both
	// are set, per-user RBAC authentication supersedes the single shared
	// password and require-pass is ignored.
	if cfg.GetRBACConfig() != "" && cfg.GetRequirePass() != "" {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "both --rbac-config and --require-pass are set",
				log.String("auth", "RBAC authentication supersedes require-pass"))
		}
	}
	if cfg.MTLSEnabled() {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "Transport security",
				log.String("tls", "ENABLED"),
				log.String("mode", "mTLS"),
				log.String("cert", cfg.GetTLSCert()),
				log.String("ca", cfg.GetTLSCA()),
			)
		}
	} else if cfg.TLSEnabled() {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "Transport security",
				log.String("tls", "ENABLED"),
				log.String("mode", "TLS"),
				log.String("cert", cfg.GetTLSCert()),
			)
		}
	} else {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "Transport security", log.String("tls", "DISABLED"))
		}
	}
}

func (a *App) GetLogger() log.Logger     { return a.logger }
func (a *App) GetConfig() *config.Config { return a.config }
