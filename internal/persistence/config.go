package persistence

import (
	"os"
	"strconv"
	"time"
)

const (
	SyncModeGroupCommit  = "group_commit"
	SyncModePerCommit    = "per_commit_sync"
	SyncModeAsync        = "async"
)

type SyncConfig struct {
	Mode      string
	BatchSize int
	Timeout   time.Duration
	UseFdatasync bool
}

func loadSyncConfig() SyncConfig {
	cfg := SyncConfig{
		Mode:         SyncModeGroupCommit,
		BatchSize:    128,
		Timeout:      500 * time.Microsecond,
		UseFdatasync: true,
	}
	if v := os.Getenv("TSD_WAL_SYNC_MODE"); v != "" {
		cfg.Mode = v
	}
	if v := os.Getenv("TSD_WAL_GROUP_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
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
