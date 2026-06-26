package tellstone

import (
	"fmt"
	"strings"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
)

type App struct {
	logger log.Logger
}

func (a *App) Start(cfg *config.Config, logger log.Logger) {
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
		log.String("bind_address", cfg.Addr),
		log.String("max_msg_size", (new(config.ByteSize(cfg.MaxMsgSize))).String()),
		log.Uint32("max_msg_size_bytes", cfg.MaxMsgSize),
		log.String("evict_interval", cfg.EvictTicker.String()),
		log.Int("evict_slots", int(cfg.EvictSlots)),
	)
	if cfg.EncryptionKey != "" {
		logger.Log(log.LevelInfo, "Engine crypto status", log.String("encryption", "ENABLED (ChaCha20-Poly1305)"))
	} else {
		logger.Log(log.LevelWarn, "Engine crypto status", log.String("encryption", "DISABLED (Plaintext Mode)"))
	}
	if cfg.TraceRatio > 0 {
		logger.Log(log.LevelInfo, "Telemetry stack configuration",
			log.String("telemetry", "OTLP/gRPC Active"),
			log.Float("sample_ratio", cfg.TraceRatio),
		)
	} else {
		logger.Log(log.LevelInfo, "Telemetry stack configuration", log.String("telemetry", "NoOp Tracer"))
	}
}
