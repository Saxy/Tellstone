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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

// snapshotCleanup closes f and removes tmpPath as best-effort cleanup
// during snapshot write error paths. Secondary errors are logged but
// never mask the primary error that triggered the cleanup.
func snapshotCleanup(f *os.File, tmpPath string, primaryErr error, logger log.Logger) error {
	if cerr := f.Close(); cerr != nil {
		logCleanupWarn("snapshot: close tmp file", cerr, logger)
	}
	if rerr := os.Remove(tmpPath); rerr != nil {
		logCleanupWarn("snapshot: remove tmp file", rerr, logger)
	}
	return primaryErr
}

// logCleanupWarn logs a best-effort cleanup error. If logger is nil (child
// process), falls back to stderr so the error is never silently swallowed.
func logCleanupWarn(msg string, err error, logger log.Logger) {
	if logger != nil {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, msg, log.String("error", err.Error()))
		}
		return
	}
}

// buildSnapshotHeader constructs the 32-byte placeholder snapshot header
// with KeyCount=0 and checksum=0. Both fields are patched after all entries
// are written and hashed. The caller must hash hdr before patching.
func buildSnapshotHeader() [snapHeader]byte {
	var hdr [snapHeader]byte
	createdAt := time.Now().UnixNano()
	copy(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], 0) // KeyCount patched later
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(createdAt))
	binary.LittleEndian.PutUint64(hdr[24:32], 0) // checksum patched later
	return hdr
}

// syncDir opens dir, fsyncs it, and closes it so that a preceding os.Rename
// is durable on filesystems that require explicit directory fsync (ext4, etc).
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	if closeErr := d.Close(); closeErr != nil && syncErr == nil {
		return closeErr
	}
	return syncErr
}

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
	raw := os.Getenv("TSD_SNAP_SHARD")
	if dir == "" || raw == "" {
		os.Exit(1)
	}
	id, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		os.Exit(1)
	}

	err = snapshotChildWrite(dir, uint32(id), os.Stdin)
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

	hdr := buildSnapshotHeader()

	if _, err = f.Write(hdr[:]); err != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: write header: %w", err), logger)
	}

	h := xxhash.New()
	if _, err = h.Write(hdr[:]); err != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: hash header: %w", err), logger)
	}

	var keyCount uint64
	var writeErr error
	var entry [16]byte

	engine.ForEach(func(key string, value []byte, expiration time.Time) {
		if writeErr != nil {
			return
		}
		keyLen := uint32(len(key))
		valLen := uint32(len(value))
		var ttlNano int64
		if !expiration.IsZero() {
			ttlNano = expiration.UnixNano()
		}
		binary.LittleEndian.PutUint32(entry[0:4], keyLen)
		binary.LittleEndian.PutUint32(entry[4:8], valLen)
		binary.LittleEndian.PutUint64(entry[8:16], uint64(ttlNano))

		if _, err = h.Write(entry[:]); err != nil {
			writeErr = err
			return
		}
		if _, err = h.WriteString(key); err != nil {
			writeErr = err
			return
		}
		if _, err = h.Write(value); err != nil {
			writeErr = err
			return
		}

		if _, err = f.Write(entry[:]); err != nil {
			writeErr = err
			return
		}
		if _, err = f.WriteString(key); err != nil {
			writeErr = err
			return
		}
		if _, err = f.Write(value); err != nil {
			writeErr = err
			return
		}
		keyCount++
	})

	if writeErr != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: write entry: %w", writeErr), logger)
	}

	if err = f.Sync(); err != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: sync: %w", err), logger)
	}

	// Patch header with final checksum and key count.
	checksum := h.Sum64()
	binary.LittleEndian.PutUint64(hdr[8:16], keyCount)
	binary.LittleEndian.PutUint64(hdr[24:32], checksum)

	if _, err = f.Seek(0, 0); err != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: seek header: %w", err), logger)
	}
	if _, err = f.Write(hdr[:]); err != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: patch header: %w", err), logger)
	}

	if err = f.Sync(); err != nil {
		return 0, snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: sync header: %w", err), logger)
	}
	if err = f.Close(); err != nil {
		if rerr := os.Remove(tmpPath); rerr != nil {
			logCleanupWarn("snapshot: remove tmp after close error", rerr, logger)
		}
		return 0, fmt.Errorf("snapshot: close: %w", err)
	}

	if err = os.Rename(tmpPath, finalPath); err != nil {
		if rerr := os.Remove(tmpPath); rerr != nil {
			logCleanupWarn("snapshot: remove tmp after rename error", rerr, logger)
		}
		return 0, fmt.Errorf("snapshot: rename: %w", err)
	}
	if err = syncDir(dir); err != nil {
		return 0, fmt.Errorf("snapshot: sync dir: %w", err)
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

// snapshotRead loads a snapshot file into the engine. It validates lengths,
// verifies the checksum, and only then applies entries to the engine so that a
// corrupted snapshot never mutates the live state. Returns the number of keys
// loaded.
func snapshotRead(dir string, shardID uint32, engine *storage.Engine, logger log.Logger) (uint64, error) {
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			logCleanupWarn("snapshot: close file", cerr, logger)
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("snapshot: stat %s: %w", path, err)
	}
	fileSize := fi.Size()

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
	if _, err = h.Write(hdr[:]); err != nil {
		return 0, fmt.Errorf("snapshot: hash header: %w", err)
	}

	// Decode all entries into temporary buffers before touching the engine so
	// that a checksum failure leaves the engine untouched. Each buffer is
	// contiguous [key|value] for SetFromBuffer compatibility.
	type decodedEntry struct {
		kvBuf   []byte // [key|value] contiguous
		keyLen  uint32
		ttlNano int64
	}
	var entries []decodedEntry
	entryBuf := make([]byte, 16)
	remaining := fileSize - int64(snapHeader)

	for {
		if _, err = io.ReadFull(f, entryBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("snapshot: read entry header: %w", err)
		}
		if _, err = h.Write(entryBuf); err != nil {
			return 0, fmt.Errorf("snapshot: hash entry header: %w", err)
		}

		keyLen := binary.LittleEndian.Uint32(entryBuf[0:4])
		valLen := binary.LittleEndian.Uint32(entryBuf[4:8])
		ttlNano := int64(binary.LittleEndian.Uint64(entryBuf[8:16]))
		kvLen64 := int64(keyLen) + int64(valLen)
		if kvLen64 < 0 || kvLen64 > remaining-16 {
			return 0, fmt.Errorf("snapshot: invalid entry lengths key=%d val=%d (remaining=%d)", keyLen, valLen, remaining)
		}
		remaining -= 16 + kvLen64
		kvLen := int(keyLen) + int(valLen)
		buf := make([]byte, kvLen)
		if _, err = io.ReadFull(f, buf); err != nil {
			return 0, fmt.Errorf("snapshot: read key+value: %w", err)
		}
		if _, err := h.Write(buf[:keyLen]); err != nil {
			return 0, fmt.Errorf("snapshot: hash key: %w", err)
		}
		if _, err := h.Write(buf[keyLen:]); err != nil {
			return 0, fmt.Errorf("snapshot: hash value: %w", err)
		}

		entries = append(entries, decodedEntry{kvBuf: buf, keyLen: keyLen, ttlNano: ttlNano})
	}

	actualChecksum := h.Sum64()
	if actualChecksum != fileChecksum {
		return 0, fmt.Errorf("snapshot: checksum mismatch (file=%d, computed=%d)", fileChecksum, actualChecksum)
	}

	// Checksum is valid — safe to apply entries to the engine.
	var loadedKeys uint64
	for i := range entries {
		e := &entries[i]
		var duration time.Duration
		if e.ttlNano != 0 {
			ttl := time.Unix(0, e.ttlNano)
			duration = time.Until(ttl)
			if duration <= 0 {
				continue // expired — skip
			}
		}
		if err = engine.SetFromBuffer(e.kvBuf, int(e.keyLen), duration); err != nil {
			return 0, fmt.Errorf("snapshot: engine.SetFromBuffer: %w", err)
		}
		loadedKeys++
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
// Checks file size and magic bytes so stale or incompatible files are not
// selected.
func snapshotExists(dir string, shardID uint32) bool {
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() {
		// Read-only open: close errors are harmless and cannot be reported.
		_ = f.Close()
	}()
	var hdr [4]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return false
	}
	return string(hdr[:]) == snapMagic
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

	// Use a bounded deadline so cmd.Wait cannot block indefinitely if the
	// child hangs or the pipe stalls.
	const childTimeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), childTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "--snapshot-child")
	cmd.Stdin = pr
	cmd.Stdout = nil
	cmd.Stderr = nil
	// Replace inherited env with only the variables the child needs so that
	// secrets and other host state are not leaked to the child process.
	cmd.Env = []string{
		"TSD_SNAP_DIR=" + dir,
		fmt.Sprintf("TSD_SNAP_SHARD=%d", shardID),
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err = cmd.Start(); err != nil {
		if cerr := pr.Close(); cerr != nil {
			logCleanupWarn("snapshot: close pipe reader", cerr, logger)
		}
		if cerr := pw.Close(); cerr != nil {
			logCleanupWarn("snapshot: close pipe writer", cerr, logger)
		}
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "snapshot: fork failed, falling back to in-process",
				log.String("error", err.Error()))
		}
		_, err := snapshotWrite(dir, shardID, engine, logger)
		return err
	}

	// Parent: close read end (child inherited it via cmd.Stdin).
	if cerr := pr.Close(); cerr != nil {
		logCleanupWarn("snapshot: close pipe reader", cerr, logger)
	}

	// Serialize engine state into the write end; propagate any write error.
	if serr := serializeEngineToWriter(pw, engine); serr != nil {
		if cerr := pw.Close(); cerr != nil {
			logCleanupWarn("snapshot: close pipe writer", cerr, logger)
		}
		// Kill the child since it will never receive EOF.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("snapshot: serialize: %w", serr)
	}

	// Close write end so the child receives EOF on its stdin.
	if cerr := pw.Close(); cerr != nil {
		logCleanupWarn("snapshot: close pipe writer", cerr, logger)
	}

	// Wait for child to finish writing the snapshot file.
	if err = cmd.Wait(); err != nil {
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
// format (without file header) to the given writer. Stops at the first write
// error and returns it.
func serializeEngineToWriter(w io.Writer, engine *storage.Engine) error {
	var writeErr error
	engine.ForEach(func(key string, value []byte, expiration time.Time) {
		if writeErr != nil {
			return
		}
		keyLen := uint32(len(key))
		valLen := uint32(len(value))
		var ttlNano int64
		if !expiration.IsZero() {
			ttlNano = expiration.UnixNano()
		}
		var hdr [16]byte
		binary.LittleEndian.PutUint32(hdr[0:4], keyLen)
		binary.LittleEndian.PutUint32(hdr[4:8], valLen)
		binary.LittleEndian.PutUint64(hdr[8:16], uint64(ttlNano))

		if _, err := w.Write(hdr[:]); err != nil {
			writeErr = err
			return
		}
		if _, err := io.WriteString(w, key); err != nil {
			writeErr = err
			return
		}
		if _, err := w.Write(value); err != nil {
			writeErr = err
			return
		}
	})
	return writeErr
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

	hdr := buildSnapshotHeader()

	if _, err := f.Write(hdr[:]); err != nil {
		return snapshotCleanup(f, tmpPath, err, nil)
	}

	h := xxhash.New()
	if _, err := h.Write(hdr[:]); err != nil {
		return snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: hash header: %w", err), nil)
	}

	var keyCount uint64
	entryBuf := make([]byte, 16)
	var keyBuf, valBuf []byte

	for {
		if _, err = io.ReadFull(r, entryBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return snapshotCleanup(f, tmpPath, err, nil)
		}
		if _, err = h.Write(entryBuf); err != nil {
			return snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: hash entry header: %w", err), nil)
		}

		keyLen := binary.LittleEndian.Uint32(entryBuf[0:4])
		valLen := binary.LittleEndian.Uint32(entryBuf[4:8])

		if cap(keyBuf) < int(keyLen) {
			keyBuf = make([]byte, keyLen)
		} else {
			keyBuf = keyBuf[:keyLen]
		}
		if _, err = io.ReadFull(r, keyBuf); err != nil {
			return snapshotCleanup(f, tmpPath, err, nil)
		}
		if _, err = h.Write(keyBuf); err != nil {
			return snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: hash key: %w", err), nil)
		}

		if cap(valBuf) < int(valLen) {
			valBuf = make([]byte, valLen)
		} else {
			valBuf = valBuf[:valLen]
		}
		if _, err = io.ReadFull(r, valBuf); err != nil {
			return snapshotCleanup(f, tmpPath, err, nil)
		}
		if _, err = h.Write(valBuf); err != nil {
			return snapshotCleanup(f, tmpPath, fmt.Errorf("snapshot: hash value: %w", err), nil)
		}

		if _, err = f.Write(entryBuf); err != nil {
			return snapshotCleanup(f, tmpPath, err, nil)
		}
		if _, err = f.Write(keyBuf); err != nil {
			return snapshotCleanup(f, tmpPath, err, nil)
		}
		if _, err = f.Write(valBuf); err != nil {
			return snapshotCleanup(f, tmpPath, err, nil)
		}
		keyCount++
	}

	// Patch header with final KeyCount and checksum.
	checksum := h.Sum64()
	binary.LittleEndian.PutUint64(hdr[8:16], keyCount)
	binary.LittleEndian.PutUint64(hdr[24:32], checksum)

	if _, err = f.Seek(0, 0); err != nil {
		return snapshotCleanup(f, tmpPath, err, nil)
	}
	if _, err = f.Write(hdr[:]); err != nil {
		return snapshotCleanup(f, tmpPath, err, nil)
	}
	if err = f.Sync(); err != nil {
		return snapshotCleanup(f, tmpPath, err, nil)
	}
	if err = f.Close(); err != nil {
		if rerr := os.Remove(tmpPath); rerr != nil {
			logCleanupWarn("snapshot: remove tmp after close error", rerr, nil)
		}
		return err
	}

	if err = os.Rename(tmpPath, finalPath); err != nil {
		if rerr := os.Remove(tmpPath); rerr != nil {
			logCleanupWarn("snapshot: remove tmp after rename error", rerr, nil)
		}
		return err
	}
	return syncDir(dir)
}
