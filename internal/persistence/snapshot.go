/*
Package persistence
Tellstone Cloud-Native In-Memory Database
File: snapshot.go
Description: Binary snapshot format and fork-based compaction. A snapshot captures a
shard's in-memory engine state in a compact binary file, allowing the WAL to be
truncated. On startup, the snapshot is loaded first (fast binary read), then only
the small post-snapshot WAL is replayed.

The snapshot child is spawned via ForkExec (same binary, --snapshot-child flag).
The parent serializes the engine map to a pipe under a brief read lock, then the
child writes the snapshot file to disk. This keeps the parent's lock duration
minimal while the child handles the heavy I/O.

Authors:

	Maximilian Hagen
*/
package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
	"github.com/cespare/xxhash/v2"
)

const (
	snapMagic   = "TSNS"
	snapVersion = 1
	snapHeader  = 32 // magic(4) + version(4) + keyCount(8) + createdAt(8) + checksum(8)
)

// IsSnapshotChild returns true when the process was spawned as a snapshot child.
// Check this early in main() and redirect to snapshotChildMain().
func IsSnapshotChild() bool {
	for _, arg := range os.Args[1:] {
		if arg == "--snapshot-child" {
			return true
		}
	}
	return false
}

// SnapshotChildMain runs in the forked child process. It reads serialized
// engine entries from stdin, writes the snapshot file, and exits.
// The dir and shardID are passed via environment variables set by the parent.
func SnapshotChildMain() {
	dir := os.Getenv("TSD_SNAP_DIR")
	shardID := 0
	fmt.Sscanf(os.Getenv("TSD_SNAP_SHARD"), "%d", &shardID)
	if dir == "" || shardID < 0 {
		os.Exit(1)
	}

	err := snapshotChildWrite(dir, uint32(shardID), os.Stdin)
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

// snapshotWrite serializes all live entries from the engine into a snapshot file.
// Writes to a temporary file first, then atomically renames over the target.
// Returns the number of keys written.
func snapshotWrite(dir string, shardID uint32, engine *storage.Engine, logger log.Logger) (uint64, error) {
	tmpPath := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap.tmp", shardID))
	finalPath := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return 0, fmt.Errorf("snapshot: create %s: %w", tmpPath, err)
	}

	createdAt := time.Now().UnixNano()

	// Placeholder header: KeyCount=0, createdAt=real, checksum=0.
	// Must match what snapshotRead hashes: it zeroes the checksum slot and
	// hashes the full 32 bytes, so we hash the same layout here.
	var hdr [snapHeader]byte
	copy(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], 0) // patched later
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(createdAt))
	binary.LittleEndian.PutUint64(hdr[24:32], 0) // patched later

	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("snapshot: write header: %w", err)
	}

	h := xxhash.New()
	h.Write(hdr[:]) // hash placeholder header (KeyCount=0, checksum=0)

	var keyCount uint64
	var entry [16]byte

	engine.ForEach(func(key string, value []byte, expiration time.Time) {
		keyLen := uint32(len(key))
		valLen := uint32(len(value))
		var ttlNano int64
		if !expiration.IsZero() {
			ttlNano = expiration.UnixNano()
		}
		binary.LittleEndian.PutUint32(entry[0:4], keyLen)
		binary.LittleEndian.PutUint32(entry[4:8], valLen)
		binary.LittleEndian.PutUint64(entry[8:16], uint64(ttlNano))

		h.Write(entry[:])
		h.WriteString(key)
		h.Write(value)

		f.Write(entry[:])
		f.WriteString(key)
		f.Write(value)
		keyCount++
	})

	if err = f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("snapshot: sync: %w", err)
	}

	// Patch header with final checksum and key count.
	checksum := h.Sum64()
	binary.LittleEndian.PutUint64(hdr[8:16], keyCount)
	binary.LittleEndian.PutUint64(hdr[24:32], checksum)

	f.Seek(0, 0)
	if _, err = f.Write(hdr[:]); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("snapshot: patch header: %w", err)
	}

	if err = f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("snapshot: sync header: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("snapshot: rename: %w", err)
	}

	if logger != nil && logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "snapshot: written",
			log.Uint("shard", shardID),
			log.Uint64("keys", keyCount),
			log.String("path", finalPath),
		)
	}
	return keyCount, nil
}

// snapshotRead loads a snapshot file into the engine. Verifies magic, version,
// and checksum before applying entries. Returns the number of keys loaded.
func snapshotRead(dir string, shardID uint32, engine *storage.Engine, logger log.Logger) (uint64, error) {
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	defer f.Close()

	var hdr [snapHeader]byte
	if _, err = io.ReadFull(f, hdr[:]); err != nil {
		return 0, fmt.Errorf("snapshot: read header: %w", err)
	}
	if string(hdr[0:4]) != snapMagic {
		return 0, fmt.Errorf("snapshot: invalid magic %q (want %q)", string(hdr[0:4]), snapMagic)
	}
	version := binary.LittleEndian.Uint32(hdr[4:8])
	if version != snapVersion {
		return 0, fmt.Errorf("snapshot: unsupported version %d (want %d)", version, snapVersion)
	}
	fileKeyCount := binary.LittleEndian.Uint64(hdr[8:16])
	fileChecksum := binary.LittleEndian.Uint64(hdr[24:32])

	// Compute checksum over (header with KeyCount=0, checksum=0) + all entries.
	// This matches the writer: it hashes the placeholder header (KeyCount=0,
	// checksum=0) before patching the real values. We must hash the same bytes.
	h := xxhash.New()
	binary.LittleEndian.PutUint64(hdr[8:16], 0)
	binary.LittleEndian.PutUint64(hdr[24:32], 0)
	h.Write(hdr[:])

	var loadedKeys uint64
	entryBuf := make([]byte, 16)
	var kvBuf []byte

	for {
		if _, err = io.ReadFull(f, entryBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return 0, fmt.Errorf("snapshot: read entry header: %w", err)
		}
		h.Write(entryBuf)

		keyLen := binary.LittleEndian.Uint32(entryBuf[0:4])
		valLen := binary.LittleEndian.Uint32(entryBuf[4:8])
		ttlNano := int64(binary.LittleEndian.Uint64(entryBuf[8:16]))

		kvLen := int(keyLen) + int(valLen)
		if cap(kvBuf) < kvLen {
			kvBuf = make([]byte, kvLen)
		} else {
			kvBuf = kvBuf[:kvLen]
		}
		if _, err = io.ReadFull(f, kvBuf); err != nil {
			return 0, fmt.Errorf("snapshot: read key+value: %w", err)
		}
		// Hash key and value separately to match the writer's layout.
		h.Write(kvBuf[:keyLen])
		h.Write(kvBuf[keyLen:])

		// Compute TTL duration relative to now.
		var duration time.Duration
		if ttlNano != 0 {
			ttl := time.Unix(0, ttlNano)
			duration = time.Until(ttl)
			if duration <= 0 {
				continue // expired — skip
			}
		}

		// SetFromBuffer takes ownership of kvBuf — allocate a fresh one on
		// the next iteration so the engine's map entry remains valid.
		snap := kvBuf
		kvBuf = nil
		if err = engine.SetFromBuffer(snap, int(keyLen), duration); err != nil {
			return 0, fmt.Errorf("snapshot: engine.SetFromBuffer: %w", err)
		}
		loadedKeys++
	}

	actualChecksum := h.Sum64()
	if fileChecksum != 0 && actualChecksum != fileChecksum {
		return 0, fmt.Errorf("snapshot: checksum mismatch (file=%d, computed=%d)", fileChecksum, actualChecksum)
	}

	if logger != nil && logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "snapshot: loaded",
			log.Uint("shard", shardID),
			log.Uint64("keys", loadedKeys),
			log.Uint64("declared_keys", fileKeyCount),
		)
	}
	return loadedKeys, nil
}

// snapshotExists reports whether a valid snapshot file exists for the shard.
func snapshotExists(dir string, shardID uint32) bool {
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))
	fi, err := os.Stat(path)
	return err == nil && fi.Size() >= snapHeader
}

// snapshotForkDump triggers a fork-based snapshot. The parent serializes the
// engine map to a pipe under a brief read lock, then ForkExec's the same binary
// with --snapshot-child. The child reads from the pipe and writes the snapshot
// file. If fork fails, falls back to an in-process write.
func snapshotForkDump(dir string, shardID uint32, engine *storage.Engine, logger log.Logger) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		_, err = snapshotWrite(dir, shardID, engine, logger)
		return err
	}

	cmd := exec.Command(os.Args[0], "--snapshot-child")
	cmd.Stdin = pr
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Env = append(os.Environ(),
		"TSD_SNAP_DIR="+dir,
		fmt.Sprintf("TSD_SNAP_SHARD=%d", shardID),
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err = cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "snapshot: fork failed, falling back to in-process",
				log.String("error", err.Error()))
		}
		_, err := snapshotWrite(dir, shardID, engine, logger)
		return err
	}

	// Parent: close read end (child inherited it via cmd.Stdin).
	pr.Close()

	// Serialize engine state into the write end under a brief read lock.
	serializeEngineToWriter(pw, engine)

	// Close write end so the child receives EOF on its stdin.
	pw.Close()

	// Wait for child to finish writing the snapshot file.
	if err := cmd.Wait(); err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "snapshot: child process failed",
				log.String("error", err.Error()))
		}
		return fmt.Errorf("snapshot: child: %w", err)
	}

	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "snapshot: fork-based dump complete",
			log.Uint("shard", shardID))
	}
	return nil
}

// serializeEngineToWriter writes all engine entries in the snapshot binary
// format (without file header) to the given writer.
func serializeEngineToWriter(w io.Writer, engine *storage.Engine) {
	var entry [16]byte

	engine.ForEach(func(key string, value []byte, expiration time.Time) {
		keyLen := uint32(len(key))
		valLen := uint32(len(value))
		var ttlNano int64
		if !expiration.IsZero() {
			ttlNano = expiration.UnixNano()
		}
		binary.LittleEndian.PutUint32(entry[0:4], keyLen)
		binary.LittleEndian.PutUint32(entry[4:8], valLen)
		binary.LittleEndian.PutUint64(entry[8:16], uint64(ttlNano))

		w.Write(entry[:])
		io.WriteString(w, key)
		w.Write(value)
	})
}

// snapshotChildWrite reads serialized entries from r and writes the snapshot
// file. Called by the child process after ForkExec.
func snapshotChildWrite(dir string, shardID uint32, r io.Reader) error {
	tmpPath := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap.tmp", shardID))
	finalPath := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	createdAt := time.Now().UnixNano()

	// Build placeholder header: Magic + Version set, KeyCount=0, createdAt=real, checksum=0.
	// This matches what snapshotRead hashes: it reads the on-disk header, zeroes the
	// checksum slot (24:32), then hashes the full 32 bytes. We must hash the same
	// bytes — header with real createdAt but zero KeyCount and zero checksum.
	var hdr [snapHeader]byte
	copy(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], 0) // KeyCount patched later
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(createdAt))
	binary.LittleEndian.PutUint64(hdr[24:32], 0) // checksum patched later

	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	h := xxhash.New()
	h.Write(hdr[:]) // hash the placeholder header (KeyCount=0, checksum=0)

	var keyCount uint64
	entryBuf := make([]byte, 16)
	var keyBuf, valBuf []byte

	for {
		if _, err := io.ReadFull(r, entryBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		h.Write(entryBuf)

		keyLen := binary.LittleEndian.Uint32(entryBuf[0:4])
		valLen := binary.LittleEndian.Uint32(entryBuf[4:8])

		if cap(keyBuf) < int(keyLen) {
			keyBuf = make([]byte, keyLen)
		} else {
			keyBuf = keyBuf[:keyLen]
		}
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		h.Write(keyBuf)

		if cap(valBuf) < int(valLen) {
			valBuf = make([]byte, valLen)
		} else {
			valBuf = valBuf[:valLen]
		}
		if _, err := io.ReadFull(r, valBuf); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		h.Write(valBuf)

		f.Write(entryBuf)
		f.Write(keyBuf)
		f.Write(valBuf)
		keyCount++
	}

	// Patch header with final KeyCount and checksum.
	checksum := h.Sum64()
	binary.LittleEndian.PutUint64(hdr[8:16], keyCount)
	binary.LittleEndian.PutUint64(hdr[24:32], checksum)

	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if _, err = f.Write(hdr[:]); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	return os.Rename(tmpPath, finalPath)
}
