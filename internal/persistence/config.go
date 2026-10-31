package persistence

import (
	"os"
	"strconv"
	"time"
)

const (
	SyncModeGroupCommit = "group_commit"
	SyncModePerCommit   = "per_commit_sync"
)

type SyncConfig struct {
	Mode          string
	BatchSize     int
	Timeout       time.Duration
	UseFdatasync  bool
}

func loadSyncConfig() SyncConfig {
	cfg := SyncConfig{
		Mode:         SyncModeGroupCommit,
		BatchSize:    2048,
		Timeout:      100 * time.Microsecond,
		UseFdatasync: true,
	}
	if v := os.Getenv("TSD_WAL_SYNC_MODE"); v != "" {
		switch v {
		case SyncModeGroupCommit, SyncModePerCommit:
			cfg.Mode = v
		default:
			// Unsupported/invalid modes fall back to per-commit for safety (do not silently use async)
			cfg.Mode = SyncModePerCommit
		}
	}
	if v := os.Getenv("TSD_WAL_GROUP_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 4096 {
				n = 4096
			}
			cfg.BatchSize = n
		}
	}
	if v := os.Getenv("TSD_WAL_GROUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Timeout = d
		}
	}
	if v := os.Getenv("TSD_WAL_USE_FDATASYNC"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.UseFdatasync = b
		}
	}
	return cfg
}
