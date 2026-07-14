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
                                                                                                    
                                                %#%%                                                
                                             %#######%%                                             
                                          ##############%%                                          
                                       ####################%%                                       
                                    ##########################%%                                    
                                %################################%%%                                
                             ###################+==+################%%%                             
                          ######################+==+####################%%                          
                       ##########################==########################%%                       
                    #############################==###########################%%                    
                 ###########*++*#################==##################**##########%%                 
               #############====*################==#################====############%               
               #############*+===+*##############==###############*===+##########%%%%      Tellsone
               ################*===+#############==*************+==+#########%%%%%%%%      >> TSD CORE ENGINE <<     
               ##################*===*###########==+++++++++++====*#########%%%%%%%%%      https://github.com/Saxy/Tellstone       
               ###################====+*#########==##########+==*######%%%%%%%%%%%%%%               
               ###################==*+==+*######*==########+==+#####%%%%%%%%%%%%%%%%%               
               ###################==##*+==+**+========+##+==+####%%%%%%%%%%%%%%%%%%%%               
               ###################==####*+====+*####*+====+##%%%%%%%%%%%%%%%%%%%%%%%%               
               ###################==#####+==*%@@@@@@@@%*==*%%%%%%%%%%%%%%%%%%%%%%%%%%               
               ###################==####+==#@@@#*++*#@@@#==*%%%%%%%%%%%%%%%%%%%%%%%%%               
               ###################==####==*@@@+======+@@@*==%%%%%%%%%%%%%%%%%%%%%%%%%               
               ######+====================#@@@========@@@#====================+%%%%%%               
               #########################==*@@@+======+@@@*==%%%%==%%%%%%%%%%%%%%%%%%%               
               #########################+==#@@@#*++*#@@@#==*%%%%==%%%%%%%%%%%%%%%%%%%               
               #########################%+==*%@@@@@@@@%*==*%%%%%==%%%%%%%%%%%%%%%%%%%               
               #####################%%%%#+====+*####*+====+%@@%%==%%%%%%%%%%%%%%%%%%%               
               ##################%%%%%%+==*%#*+======+*%%*==*%@@==%%%%%%%%#%%%%%%%%%%               
               ###############%%%%%%%*==+%%%%%%%%==%@@@@@@%*==*%==@@@%%%%%%#%%%%%%%%%               
               ############%%%%%%%%*==+#%%%%%%%%%==@@@@@@@@@%+====@@@@@@%%%%%%%%%%%%%               
               ###########%%%%%%%*===++++++++++++==@@@@@@@@@@@#+==#@@@@@@%%%%%%%%%%%%               
               #####%%%%%%%%%%%#+==+#############==@@@@@@@@@@@@@*==+%@%%%%%@@@%%%%%%%               
               ##%%%%%%%%%%%#*+==*%%%%%%%%%%%%%%%==@@@@@@@@@@@@@@@*==+*%@@@@@@@@@@%%%               
               %%%%%%%%%%%%%====#%%%%%%%%%%%%%%%%==@@@@@@@@@@@@@%%%%===+@@@@@@@@@@@@%               
                 %%%%%%%%%%%#**#%%%%%%%%%%%%%%%%%==@@@@@@@@@@%%%%%@@%##%@@@@@@@@@@@                 
                    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%==@@@@@@@%%%%%@@@@@@@@@@@@@@@@@                    
                       %%%%%%%%%%%%%%%%%%%%%%%%%%==@@@@%%%%%@@@@@@@@@@@@@@@@@                       
                          %%%%%%%%%%%%%%%%%%%%%%+==+%%%%%@@@@@@@@@@@@@@@@@                          
                             %%%%%%%%%%%%%%%%%%%+==+%%@@@@@@@@@@@@@@@@@                             
                                @%%%%%%%%%%%%%%%%%%@@@@@@@@@@@@@@@@@                                
                                    %%%%%%%%%%%%%%@@@@@@@@@@@@@@                                    
                                       %%%%%%%%%%%@@@@@@@@@@@                                       
                                          %%%%%%%%@@@@@@@@                                          
                                             %%%%%@@@@@                                             
                                                @%@@
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
		if cfg.GetEncryptionKey() == "" {
			if logger.Enabled(log.LevelFatal) {
				logger.Log(log.LevelFatal, "Encryption key must be provided", log.String("error", "encryption key is missing but encryption is enabled"))
			}
		}
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "Engine crypto status", log.String("encryption", "ENABLED (ChaCha20-Poly1305)"))
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
}

func (a *App) GetLogger() log.Logger     { return a.logger }
func (a *App) GetConfig() *config.Config { return a.config }
