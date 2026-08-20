package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
)

const (
	osWindows = "windows"
	osDarwin  = "darwin"

	// walMagic marks the start of an encrypted WAL file. Plaintext WALs have
	// no magic — they begin directly with the first record's keyLen field.
	// The trailing byte is the WAL format version (currently 1).
	walMagic    = "TSW\x01"
	walMagicLen = 4
)

var tombstoneTTL int64 = math.MinInt64

func getDefaultDir() string {
	var baseDir string
	switch runtime.GOOS {
	case osWindows:
		baseDir = os.Getenv("APPDATA")
	case osDarwin:
		baseDir = filepath.Join(os.Getenv("HOME"), "Library", "Application Support")
	default:
		baseDir = filepath.Join(os.Getenv("HOME"), ".local", "share")
	}
	return filepath.Join(baseDir, "tellstone", "data")
}

// nonceSidecarPath returns the path for the nonce counter sidecar file.
// The sidecar persists the high-water nonce counter so it survives WAL loss.
func nonceSidecarPath(dir string, shardID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("shard_%03d.nonce", shardID))
}

// readNonceSidecar reads the persisted nonce counter from the sidecar file.
// Returns 0 if the file does not exist or is invalid.
func readNonceSidecar(path string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer func(f *os.File) { _ = f.Close() }(f)
	var buf [8]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(buf[:])
}

// writeNonceSidecar persists the nonce counter to a sidecar file using an
// atomic write sequence: temp file → sync → rename → dir sync. This ensures
// the sidecar is never left in a partial state after a crash. The counter
// represents the next value to use (high-water mark + 1).
func writeNonceSidecar(path string, ctr uint64) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("persistence: create nonce sidecar tmp: %w", err)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], ctr)
	if _, err = f.Write(buf[:]); err != nil {
		if cerr := f.Close(); cerr != nil {
			_ = os.Remove(tmpPath)
		} else {
			_ = os.Remove(tmpPath)
		}
		return fmt.Errorf("persistence: write nonce sidecar: %w", err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persistence: sync nonce sidecar: %w", err)
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persistence: close nonce sidecar: %w", err)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persistence: rename nonce sidecar: %w", err)
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("persistence: sync dir for nonce sidecar: %w", err)
	}
	return nil
}

// shardHandle holds the WAL file and its per-shard mutex for a single shard.
// When encryption is enabled, crypto holds the DEK engine and nonceCtr tracks
// the next counter-based nonce value. The counter is durable: it is recovered
// from the WAL on replay so nonces never repeat across crashes.
type shardHandle struct {
	file          *os.File
	mu            sync.Mutex
	crypto        *crypto.Engine
	walVer        uint8
	nonceCtr      atomic.Uint64
	shardID       uint32
	needsMigrate  bool           // plaintext WAL opened with crypto — migrate after replay
	pendingCrypto *crypto.Engine // crypto engine waiting for migration
}

// Storage provides a per-shard, append-only write-ahead log (WAL) for crash recovery.
// Each shard owns an independent file, eliminating cross-shard coordination during writes.
// When disabled, Write and Delete are no-ops and no files are opened.
type Storage struct {
	dir     string
	enabled bool
	logger  log.Logger
	shards  map[uint32]*shardHandle
	mapMu   sync.RWMutex
}

// NewStorage creates a new persistence Storage. If enabled is false, a pass-through
// (no-op) instance is returned. If the dir is empty, the platform-specific default is used.
// Returns an error if enabled is true and the data directory cannot be created.
func NewStorage(enabled bool, logger log.Logger, dir string) (*Storage, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	if dir == "" {
		dir = getDefaultDir()
		if logger.Enabled(log.LevelDebug) {
			logger.Log(log.LevelDebug, "no persistence dir configured, using default", log.String("dir", dir))
		}
	}
	if !enabled {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "data storage initialized in pass-through mode (data storage disabled)")
		}
		return &Storage{
			enabled: false,
			logger:  logger,
		}, nil
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "data storage initialized", log.String("dir", dir))
	}
	stg := &Storage{
		dir:     dir,
		enabled: true,
		logger:  logger,
		shards:  make(map[uint32]*shardHandle),
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "persistence: failed to create data directory",
				log.String("dir", dir), log.String("error", err.Error()))
		}
		return nil, fmt.Errorf("persistence: mkdir %s: %w", dir, err)
	}
	if logger.Enabled(log.LevelDebug) {
		logger.Log(log.LevelDebug, "persistence: data directory ready", log.String("dir", dir))
	}
	return stg, nil
}

// Enabled reports whether this storage instance will actually write to disk.
func (s *Storage) Enabled() bool {
	return s.enabled
}

// getShard retrieves the shard handle under mapMu. Returns nil if the shard
// has not been opened.
func (s *Storage) getShard(shardID uint32) *shardHandle {
	s.mapMu.RLock()
	h := s.shards[shardID]
	s.mapMu.RUnlock()
	return h
}

// appendRecord is the shared writing path for both SET and tombstone records.
// It acquires the shard mutex, writes the header, key, value, and syncs.
// When the shard's WAL version is 1 (encrypted), the entire plaintext record
// is encrypted with a counter-based nonce before writing.
func (s *Storage) appendRecord(shardID uint32, header [16]byte, key string, value []byte, op string) error {
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.needsMigrate {
		return fmt.Errorf("persistence: shard %d migration in progress, writes blocked", shardID)
	}

	if h.walVer == 1 {
		return s.appendRecordEncrypted(h, shardID, header, key, value, op)
	}

	if _, err := h.file.Write(header[:]); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write "+op+" header failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.WriteString(key); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write "+op+" key failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.Write(value); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write "+op+" value failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := h.file.Sync(); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync "+op+" failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	return nil
}

// appendRecordEncrypted writes an encrypted WAL record. Format on disk:
//
//	[recLen:4B LE][nonce:12B][ciphertext + tag]
//
// The nonce is [counter:8B LE][shardID:4B LE]. The counter is atomic and
// recovered from WAL replay on restart so nonces never repeat across crashes.
func (s *Storage) appendRecordEncrypted(h *shardHandle, shardID uint32, header [16]byte, key string, value []byte, op string) error {
	// Assemble plaintext record: header + key + value.
	plainLen := 16 + len(key) + len(value)
	plaintext := make([]byte, plainLen)
	copy(plaintext, header[:])
	copy(plaintext[16:], key)
	copy(plaintext[16+len(key):], value)

	// Build counter-based nonce: [counter:8B][shardID:4B].
	ctr := h.nonceCtr.Add(1) - 1
	var nonce [12]byte
	binary.LittleEndian.PutUint64(nonce[:8], ctr)
	binary.LittleEndian.PutUint32(nonce[8:12], shardID)

	// Encrypt: ciphertext = Seal(nonce, plaintext) which includes 16-byte tag.
	ciphertext, err := h.crypto.SealWithCounter(nonce[:], plaintext)
	if err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: encrypt "+op+" failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}

	// On-disk record: [recLen:4B LE][nonce:12B][ciphertext].
	recLen := int64(12) + int64(len(ciphertext))
	if recLen > int64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: encrypted "+op+" record too large",
				log.Uint("shard", shardID), log.String("key", key), log.Int64("bytes", recLen))
		}
		return fmt.Errorf("persistence: encrypted record too large (%d bytes, max %d)", recLen, ^uint32(0))
	}
	var recLenBuf [4]byte
	binary.LittleEndian.PutUint32(recLenBuf[:], uint32(recLen))

	if _, err := h.file.Write(recLenBuf[:]); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write encrypted "+op+" length failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.Write(nonce[:]); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write encrypted "+op+" nonce failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.Write(ciphertext); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write encrypted "+op+" ciphertext failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := h.file.Sync(); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync encrypted "+op+" failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	return nil
}

// Write appends a SET record to the shard's WAL file. The record includes a
// 16-byte header (key length, value length, TTL), followed by key and value bytes.
// The writing and trailing Sync are serialized under the shard's own mutex.
// Returns nil immediately when persistence is disabled.
func (s *Storage) Write(shardID uint32, key string, value []byte, ttl time.Time) error {
	if !s.enabled {
		return nil
	}
	if uint64(len(key)) > uint64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: key too large",
				log.String("key", key), log.Int("bytes", len(key)), log.Uint("max", ^uint32(0)))
		}
		return fmt.Errorf("persistence: key too large (%d bytes, max %d)", len(key), ^uint32(0))
	}
	if uint64(len(value)) > uint64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: value too large",
				log.String("key", key), log.Int("bytes", len(value)), log.Uint("max", ^uint32(0)))
		}
		return fmt.Errorf("persistence: value too large (%d bytes, max %d)", len(value), ^uint32(0))
	}
	var header [16]byte
	var ttlNano int64
	if !ttl.IsZero() {
		ttlNano = ttl.UnixNano()
	}
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	binary.LittleEndian.PutUint64(header[8:16], uint64(ttlNano))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: write",
			log.Uint("shard", shardID), log.String("key", key),
			log.Int("key_len", len(key)), log.Int("val_len", len(value)))
	}
	return s.appendRecord(shardID, header, key, value, "set")
}

// Delete appends a tombstone record to the shard's WAL file. The tombstone
// uses the same 16-byte header format with a sentinel TTL (math.MinInt64) and
// zero-length value. During LoadShard replay, tombstones cause the key to be
// deleted from the in-memory engine.
// Returns nil immediately when persistence is disabled.
func (s *Storage) Delete(shardID uint32, key string) error {
	if !s.enabled {
		return nil
	}
	if uint64(len(key)) > uint64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: delete key too large",
				log.String("key", key), log.Int("bytes", len(key)), log.Uint("max", ^uint32(0)))
		}
		return fmt.Errorf("persistence: key too large (%d bytes, max %d)", len(key), ^uint32(0))
	}
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], 0)
	binary.LittleEndian.PutUint64(header[8:16], uint64(tombstoneTTL))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: delete",
			log.Uint("shard", shardID), log.String("key", key))
	}
	return s.appendRecord(shardID, header, key, nil, "delete")
}

// OpenShard opens (or creates) the WAL file for the given shard.
// If cryptoEng is non-nil, the WAL is encrypted with counter-based nonces and
// a 4-byte magic header is written to new files. Existing plaintext WALs
// (e.g. from v1.2.0) are transparently migrated: records are replayed first,
// then the file is truncated and re-initialized with the encrypted header.
// Must be called before Write or LoadShard for that shard.
// Returns nil immediately when persistence is disabled.
func (s *Storage) OpenShard(shardID uint32, cryptoEng *crypto.Engine) error {
	if !s.enabled {
		return nil
	}
	path := filepath.Join(s.dir, fmt.Sprintf("shard_%03d.db", shardID))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: opening shard", log.Uint("shard", shardID), log.String("path", path))
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: failed to open shard",
				log.Uint("shard", shardID), log.String("path", path), log.String("error", err.Error()))
		}
		return err
	}
	h := &shardHandle{file: f, shardID: shardID}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if fi.Size() == 0 {
		if cryptoEng != nil && cryptoEng.Enabled() {
			if _, err = f.WriteString(walMagic); err != nil {
				_ = f.Close()
				return fmt.Errorf("persistence: write WAL header: %w", err)
			}
			h.crypto = cryptoEng
			h.walVer = 1
		}
	} else {
		var magic [walMagicLen]byte
		if _, err = io.ReadFull(f, magic[:]); err != nil {
			_ = f.Close()
			return fmt.Errorf("persistence: read WAL header: %w", err)
		}
		if string(magic[:]) == walMagic {
			if cryptoEng == nil || !cryptoEng.Enabled() {
				_ = f.Close()
				return fmt.Errorf("persistence: shard %d has encrypted WAL but no crypto engine", shardID)
			}
			h.crypto = cryptoEng
			h.walVer = 1
		} else {
			// Plaintext WAL found. If encryption is requested, replay
			// the existing plaintext records first, then migrate the WAL
			// to encrypted format in-place after LoadShard completes.
			if cryptoEng != nil && cryptoEng.Enabled() {
				h.needsMigrate = true
				h.pendingCrypto = cryptoEng
				if s.logger.Enabled(log.LevelInfo) {
					s.logger.Log(log.LevelInfo, "persistence: plaintext WAL will migrate to encrypted",
						log.Uint("shard", shardID))
				}
			}
			h.walVer = 0
		}
		if _, err = f.Seek(0, 0); err != nil {
			_ = f.Close()
			return err
		}
	}
	s.mapMu.Lock()
	if old, ok := s.shards[shardID]; ok {
		_ = old.file.Close()
	}
	s.shards[shardID] = h
	s.mapMu.Unlock()
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: shard opened",
			log.Uint("shard", shardID), log.Bool("encrypted", h.walVer == 1))
	}
	return nil
}

// LoadShard restores a shard's in-memory engine. If a snapshot file exists,
// it is loaded first (fast binary read). Then the WAL is replayed on top to
// capture any writes that happened after the snapshot. This two-phase approach
// keeps warm-up fast: the snapshot is compact and the WAL is small.
func (s *Storage) LoadShard(shardID uint32, engine *storage.Engine) error {
	if !s.enabled {
		return nil
	}
	if snapshotExists(s.dir, shardID) {
		var fp [16]byte
		if h := s.getShard(shardID); h != nil && h.crypto != nil {
			fp = h.crypto.KeyFingerprint()
		}
		loadedKeys, err := snapshotRead(s.dir, shardID, engine, fp, s.logger)
		if err != nil {
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "persistence: snapshot load failed, data preceding snapshot is unrecoverable",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
			return fmt.Errorf("persistence: load shard %d snapshot: %w", shardID, err)
		}
		if s.logger.Enabled(log.LevelInfo) {
			s.logger.Log(log.LevelInfo, "persistence: snapshot restored",
				log.Uint("shard", shardID), log.Uint64("keys", loadedKeys))
		}
	}
	if err := s.replayWAL(shardID, engine); err != nil {
		return err
	}
	// After replaying a plaintext WAL with encryption enabled, migrate
	// the WAL to encrypted format in-place. The plaintext records are
	// now in memory; truncate the file, write the encrypted magic header,
	// re-serialize all records as encrypted, and switch the handle to
	// encrypted mode so new writes are sealed.
	h := s.getShard(shardID)
	if h != nil && h.needsMigrate {
		// Atomic migration: write all encrypted records to a temp file,
		// then rename it over the original. If the process crashes during
		// the rewrite, the original plaintext WAL is intact and migration
		// will run again on the next startup.
		h.mu.Lock()

		// Close the current plaintext file before swapping.
		if err := h.file.Close(); err != nil {
			h.mu.Unlock()
			return fmt.Errorf("persistence: close plaintext WAL: %w", err)
		}

		origPath := filepath.Join(s.dir, fmt.Sprintf("shard_%03d.db", shardID))
		migrPath := origPath + ".migrating"

		mf, err := os.OpenFile(migrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			// Re-open original so the handle isn't left with a closed file.
			of, openErr := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			if openErr != nil {
				h.mu.Unlock()
				return fmt.Errorf("persistence: migration failed and cannot reopen WAL: %w (original error: %v)", openErr, err)
			}
			h.file = of
			h.mu.Unlock()
			return fmt.Errorf("persistence: create migration temp file: %w", err)
		}

		// Write magic header.
		if _, err := mf.WriteString(walMagic); err != nil {
			_ = mf.Close()
			_ = os.Remove(migrPath)
			// Re-open original.
			of, _ := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			h.file = of
			h.mu.Unlock()
			return fmt.Errorf("persistence: write WAL magic for migration: %w", err)
		}

		// Build a temporary handle pointing at the migration file so
		// appendRecordEncrypted can write to it.
		tmpH := &shardHandle{
			file:    mf,
			crypto:  h.pendingCrypto,
			walVer:  1,
			shardID: shardID,
		}

		// Re-serialize every in-memory record as an encrypted WAL entry.
		var recordsWritten int
		atRestEncrypted := engine.CryptoEnabled()
		var migrateErr error
		engine.ForEach(func(key string, value []byte, expiration time.Time) {
			if migrateErr != nil {
				return
			}
			walValue := value
			if atRestEncrypted {
				plainValue, decErr := tmpH.crypto.DecryptInPlace(value)
				if decErr != nil {
					migrateErr = fmt.Errorf("persistence: migration decrypt %q: %w", key, decErr)
					return
				}
				walValue = plainValue
			}
			var header [16]byte
			var ttlNano int64
			if !expiration.IsZero() {
				ttlNano = expiration.UnixNano()
			}
			binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
			binary.LittleEndian.PutUint32(header[4:8], uint32(len(walValue)))
			binary.LittleEndian.PutUint64(header[8:16], uint64(ttlNano))
			if err = s.appendRecordEncrypted(tmpH, shardID, header, key, walValue, "migration"); err != nil {
				migrateErr = fmt.Errorf("persistence: migration write %q: %w", key, err)
				return
			}
			recordsWritten++
		})

		if migrateErr != nil {
			_ = mf.Close()
			_ = os.Remove(migrPath)
			// Re-open original so the handle is usable.
			of, _ := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			h.file = of
			h.mu.Unlock()
			return migrateErr
		}

		if err = mf.Sync(); err != nil {
			_ = mf.Close()
			_ = os.Remove(migrPath)
			of, _ := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			h.file = of
			h.mu.Unlock()
			return fmt.Errorf("persistence: sync migration file: %w", err)
		}
		if err = mf.Close(); err != nil {
			_ = os.Remove(migrPath)
			of, _ := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			h.file = of
			h.mu.Unlock()
			return fmt.Errorf("persistence: close migration file: %w", err)
		}

		// Atomic rename: original plaintext WAL is replaced by the
		// encrypted version. If the process crashes here, the original
		// is either the old file or the new file — both are valid.
		if err = os.Rename(migrPath, origPath); err != nil {
			_ = os.Remove(migrPath)
			of, _ := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			h.file = of
			h.mu.Unlock()
			return fmt.Errorf("persistence: rename migration file: %w", err)
		}
		if err := syncDir(s.dir); err != nil {
			// Best-effort: rename succeeded but dir sync failed. The
			// data is on disk; a crash+restart may or may not see it.
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "persistence: sync dir after migration failed",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
		}

		// Re-open the renamed file as the new WAL handle.
		nf, err := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			h.mu.Unlock()
			return fmt.Errorf("persistence: reopen migrated WAL: %w", err)
		}
		h.file = nf
		h.crypto = h.pendingCrypto
		h.pendingCrypto = nil
		h.walVer = 1
		h.needsMigrate = false

		// Persist the nonce counter so it survives a crash before the
		// next clean shutdown writes it during CloseShard.
		noncePath := nonceSidecarPath(s.dir, shardID)
		nonceVal := tmpH.nonceCtr.Load()
		h.nonceCtr.Store(nonceVal)
		h.mu.Unlock()

		if err = writeNonceSidecar(noncePath, nonceVal); err != nil {
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "persistence: migration nonce sidecar write failed",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
		}
		if s.logger.Enabled(log.LevelInfo) {
			s.logger.Log(log.LevelInfo, "persistence: WAL migrated from plaintext to encrypted",
				log.Uint("shard", shardID), log.Int("records", recordsWritten))
		}
	}
	return nil
}

// replayWAL replays all records from the shard's WAL file into the given engine,
// skipping expired keys and applying tombstones as deletions. Truncated records
// from a crash mid-write are detected, and the WAL is truncated to the last valid
// offset so future loads resume from a clean end.
//
// For encrypted WALs (walVer=1), each record is decrypted with its counter-based
// nonce before parsing. The nonce counter is recovered from the WAL so it is
// durable across crashes.
func (s *Storage) replayWAL(shardID uint32, engine *storage.Engine) error {
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("shard %d not opened", shardID)
	}
	h.mu.Lock()
	f := h.file
	if _, err := f.Seek(0, 0); err != nil {
		h.mu.Unlock()
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		h.mu.Unlock()
		return err
	}
	fileSize := fi.Size()
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: loading shard",
			log.Uint("shard", shardID), log.Int64("file_size", fileSize))
	}
	h.mu.Unlock()

	if h.walVer == 1 {
		return s.replayWALEncrypted(h, shardID, f, fileSize, engine)
	}
	return s.replayWALPlaintext(h, shardID, f, fileSize, engine)
}

// replayWALPlaintext replays a plaintext WAL file (walVer=0).
func (s *Storage) replayWALPlaintext(h *shardHandle, shardID uint32, f *os.File, fileSize int64, engine *storage.Engine) error {
	remaining := fileSize
	var validOffset int64
	header := make([]byte, 16)
	var recordsRead int
	var recordsSkipped int
	for {
		var n int
		n, err := io.ReadFull(f, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: incomplete header read (%d bytes): %w", n, err)
		}
		remaining -= 16
		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valLen := binary.LittleEndian.Uint32(header[4:8])
		if int64(keyLen)+int64(valLen) > remaining {
			break
		}
		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: read key: %w", err)
		}
		valBuf := make([]byte, valLen)
		if _, err = io.ReadFull(f, valBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: read value: %w", err)
		}
		remaining -= int64(keyLen) + int64(valLen)
		validOffset = fileSize - remaining
		recordsRead++
		if err = s.applyRecord(shardID, engine, header, keyBuf, valBuf); err != nil {
			if errors.Is(errRecordExpired, err) {
				recordsSkipped++
				continue
			}
			return err
		}
	}
	s.finishReplay(h, shardID, fileSize, validOffset, recordsRead, recordsSkipped)
	return nil
}

// replayWALEncrypted replays an encrypted WAL file (walVer=1). Each record is
// length-prefixed and encrypted with a counter-based nonce. The nonce counter is
// recovered from the WAL so nonces never repeat across crashes.
func (s *Storage) replayWALEncrypted(h *shardHandle, shardID uint32, f *os.File, fileSize int64, engine *storage.Engine) error {
	// Skip the WAL magic header.
	if _, err := f.Seek(walMagicLen, 0); err != nil {
		return err
	}
	remaining := fileSize - int64(walMagicLen)
	validOffset := int64(walMagicLen)
	var maxNonce uint64
	recLenBuf := make([]byte, 4)
	var recordsRead int
	var recordsSkipped int
	for {
		if _, err := io.ReadFull(f, recLenBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: read encrypted record length: %w", err)
		}
		recLen := int64(binary.LittleEndian.Uint32(recLenBuf))
		if recLen < 12+16 {
			break
		}
		remaining -= 4
		if recLen > remaining {
			break
		}
		record := make([]byte, recLen)
		if _, err := io.ReadFull(f, record); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: read encrypted record: %w", err)
		}
		remaining -= recLen
		nonce := record[:12]
		ciphertext := record[12:]
		ctr := binary.LittleEndian.Uint64(nonce[:8])
		if ctr > maxNonce {
			maxNonce = ctr
		}
		plaintext, err := h.crypto.OpenWithCounter(nonce, ciphertext)
		if err != nil {
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "persistence: decrypt failed, skipping record",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
			break
		}
		if len(plaintext) < 16 {
			break
		}
		validOffset = fileSize - remaining
		recordsRead++
		var header [16]byte
		copy(header[:], plaintext[:16])
		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valLen := binary.LittleEndian.Uint32(header[4:8])
		if int64(keyLen)+int64(valLen) != int64(len(plaintext)-16) {
			break
		}
		keyBuf := plaintext[16 : 16+keyLen]
		valBuf := plaintext[16+keyLen : 16+keyLen+valLen]
		if err = s.applyRecord(shardID, engine, header[:], keyBuf, valBuf); err != nil {
			if errors.Is(errRecordExpired, err) {
				recordsSkipped++
				continue
			}
			return err
		}
	}
	walCtr := maxNonce + 1
	sidecarCtr := readNonceSidecar(nonceSidecarPath(s.dir, shardID))
	if sidecarCtr > walCtr {
		walCtr = sidecarCtr
	}
	h.nonceCtr.Store(walCtr)
	if err := writeNonceSidecar(nonceSidecarPath(s.dir, shardID), walCtr); err != nil {
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "persistence: nonce sidecar persist failed",
				log.Uint("shard", shardID), log.String("error", err.Error()))
		}
	}
	s.finishReplay(h, shardID, fileSize, validOffset, recordsRead, recordsSkipped)
	return nil
}

var errRecordExpired = errors.New("persistence: record expired")

// applyRecord applies a single WAL record to the engine. Returns errRecordExpired
// if the record's TTL has passed (caller should count it as skipped).
func (s *Storage) applyRecord(shardID uint32, engine *storage.Engine, header, keyBuf, valBuf []byte) error {
	ttlNano := int64(binary.LittleEndian.Uint64(header[8:16]))
	if ttlNano == tombstoneTTL {
		if s.logger.Enabled(log.LevelDebug) {
			s.logger.Log(log.LevelDebug, "persistence: replaying delete",
				log.Uint("shard", shardID), log.String("key", string(keyBuf)))
		}
		engine.Delete(string(keyBuf))
		return errRecordExpired
	}
	var duration time.Duration
	if ttlNano != 0 {
		ttl := time.Unix(0, ttlNano)
		duration = time.Until(ttl)
		if duration <= 0 {
			if s.logger.Enabled(log.LevelDebug) {
				s.logger.Log(log.LevelDebug, "persistence: skipping expired key",
					log.Uint("shard", shardID), log.String("key", string(keyBuf)))
			}
			return errRecordExpired
		}
	}
	return engine.Set(string(keyBuf), valBuf, duration)
}

// finishReplay truncates a corrupted WAL tail and logs replay statistics.
func (s *Storage) finishReplay(h *shardHandle, shardID uint32, fileSize, validOffset int64, recordsRead, recordsSkipped int) {
	if validOffset < fileSize {
		h.mu.Lock()
		if err := h.file.Truncate(validOffset); err != nil {
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "persistence: failed to truncate corrupted tail",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
		} else {
			if s.logger.Enabled(log.LevelInfo) {
				s.logger.Log(log.LevelInfo, "persistence: truncated corrupted WAL tail",
					log.Uint("shard", shardID), log.Int64("from", fileSize), log.Int64("to", validOffset))
			}
		}
		h.mu.Unlock()
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: shard loaded",
			log.Uint("shard", shardID), log.Int("records_read", recordsRead),
			log.Int("records_skipped", recordsSkipped), log.Int("records_loaded", recordsRead-recordsSkipped))
	}
}

// WALSize returns the current WAL file size in bytes for the given shard.
// Returns 0 if the shard is not opened or on error.
func (s *Storage) WALSize(shardID uint32) int64 {
	h := s.getShard(shardID)
	if h == nil {
		return 0
	}
	h.mu.Lock()
	fi, err := h.file.Stat()
	h.mu.Unlock()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// Snapshot triggers a fork-based snapshot for the given shard. The parent
// serializes the engine state to a pipe, and a child process writes the
// snapshot file. After a successful snapshot, only the WAL records that were
// part of the serialized engine state are truncated; records appended during
// the snapshot are preserved so they survive a crash.
//
// The WAL boundary is captured after serialization and under the shard's WAL
// mutex so that no concurrent Write can slip between the boundary capture and
// the truncation. If the boundary cannot be obtained, truncation is skipped
// rather than risking data loss.
func (s *Storage) Snapshot(shardID uint32, engine *storage.Engine) error {
	if !s.enabled {
		return nil
	}
	var fp [16]byte
	if h := s.getShard(shardID); h != nil && h.crypto != nil {
		fp = h.crypto.KeyFingerprint()
	}
	if err := snapshotForkDump(s.dir, shardID, engine, fp, s.logger); err != nil {
		return err
	}
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	fi, err := h.file.Stat()
	if err != nil {
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "persistence: cannot determine WAL boundary, skipping truncation",
				log.Uint("shard", shardID), log.String("error", err.Error()))
		}
		return nil
	}
	return s.truncateWALLocked(h, shardID, fi.Size())
}

// truncateWALLocked persists the nonce sidecar, then truncates, syncs, and
// seeks the WAL file to the given offset. The caller must hold h.mu. Used by
// Snapshot and TruncateWALTo.
//
// The sidecar is persisted BEFORE truncation so that a crash after truncation
// never causes nonce counter regression: the sidecar already reflects the
// counter at the time of truncation, even if the WAL records are gone.
func (s *Storage) truncateWALLocked(h *shardHandle, shardID uint32, offset int64) error {
	if h.walVer == 1 {
		if err := writeNonceSidecar(nonceSidecarPath(s.dir, shardID), h.nonceCtr.Load()); err != nil {
			return fmt.Errorf("persistence: persist nonce sidecar before truncate: %w", err)
		}
	}
	if err := h.file.Truncate(offset); err != nil {
		return fmt.Errorf("persistence: truncate WAL shard %d to %d: %w", shardID, offset, err)
	}
	if err := h.file.Sync(); err != nil {
		return fmt.Errorf("persistence: sync WAL shard %d after truncate: %w", shardID, err)
	}
	if _, err := h.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("persistence: seek WAL shard %d: %w", shardID, err)
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: WAL truncated",
			log.Uint("shard", shardID), log.Int64("to", offset))
	}
	return nil
}

// TruncateWALTo truncates the WAL file to the given offset, discarding any
// bytes beyond that point. Called after a successful snapshot to remove only
// the records that were serialized, preserving later writes.
func (s *Storage) TruncateWALTo(shardID uint32, offset int64) error {
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return s.truncateWALLocked(h, shardID, offset)
}

// CloseShard closes the WAL file for the given shard.
func (s *Storage) CloseShard(shardID uint32) error {
	h := s.getShard(shardID)
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.walVer == 1 {
		if err := writeNonceSidecar(nonceSidecarPath(s.dir, shardID), h.nonceCtr.Load()); err != nil {
			return err
		}
	}
	if err := h.file.Close(); err != nil {
		return err
	}
	s.mapMu.Lock()
	if cur, ok := s.shards[shardID]; ok && cur == h {
		delete(s.shards, shardID)
	}
	s.mapMu.Unlock()
	return nil
}
