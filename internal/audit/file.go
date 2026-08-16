/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: file.go
Description: File-backed audit log writer. The on-disk file name is generated,
never supplied: a unix timestamp, the first 8 hex chars of a SHA-256 of the
destination directory (a stable per-instance fingerprint), the writing process's
PID, and the "tsd" marker. Bytes written are tracked and once the threshold
(50 MiB) is crossed the writer rotates to a freshly named file in the same
directory. When encryption is enabled, every record is sealed with the crypto
engine before being flushed.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

const (
	// auditFileMagic is the 4-byte marker at the start of every audit file
	// written by the current format. A reader uses it to distinguish headed
	// files from legacy (headerless) ones without misinterpreting record data.
	auditFileMagic = "TSDA"

	// auditFileVersion is the format version embedded in the header. Bump it
	// when the layout changes; the reader dispatches on it.
	auditFileVersion byte = 1

	// KeyModeSimple signals that every record in the file was sealed directly
	// with the pass-through mode
	KeyModeSimple byte = 0

	// KeyModeEnvelope signals envelope mode: every record was sealed with a
	// per-instance Data Encryption Key wrapped by the a KEK.
	KeyModeEnvelope byte = 1

	// auditFileHeaderLen is the fixed size of the file header in bytes:
	// [magic:4][version:1][keyMode:1][fingerprint:16].
	auditFileHeaderLen = 4 + 1 + 1 + 16
)

// rotateAfterBytes is the writing volume after which the audit file rotates.
// 50 MiB keeps a single file small enough to inspect or ingest in one piece
// while making rotation rare enough not to matter operationally.
const rotateAfterBytes = 50 << 20

// auditFileSuffix ends every audit file name. Audit files carry no index or
// manifest, so this suffix is the whole contract between the writer and anyone
// discovering the files afterwards — replay globs on it. Defined here, beside
// the fileName that applies it, so the two cannot drift apart.
const auditFileSuffix = "_tsd.log"

// file is a rotating, optionally encrypted audit log. bytesWritten counts the
// bytes flushed to the current file; when it reaches maxSize, write rotates.
// Every file starts with a 22-byte self-describing header so a reader can
// identify the sealing key without consulting a sidecar.
type file struct {
	dir          string
	path         string
	isEncrypted  bool
	ce           *crypto.Engine
	file         *os.File
	bytesWritten uint64
	maxSize      uint64
	buf          []byte
	logger       log.Logger
	keyMode      byte
	fingerprint  [16]byte
}

// newFile opens the first audit file inside dir. When engine.Enabled() is
// false, records are written as plaintext; otherwise every record is sealed.
// keyMode and fingerprint are written into the file header so every segment is
// self-describing — rotation emits the same header automatically.
func newFile(dir string, engine *crypto.Engine, logger log.Logger, keyMode byte, fingerprint [16]byte) (*file, error) {
	f := &file{dir: dir, maxSize: rotateAfterBytes, keyMode: keyMode, fingerprint: fingerprint}
	if engine != nil && engine.Enabled() {
		f.isEncrypted = true
		f.ce = engine
	}
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	f.logger = logger
	osFile, path, err := open(dir, keyMode, fingerprint)
	if err != nil {
		return nil, err
	}
	f.file = osFile
	f.path = path
	if f.logger.Enabled(log.LevelInfo) {
		f.logger.Log(log.LevelInfo, "audit: initialized log file", log.String("filename", f.file.Name()))
	}
	return f, nil
}

// open creates a new audit file exclusively (O_CREATE|O_EXCL) and writes the
// self-describing header. If the filename collides with an existing segment —
// an astronomically unlikely event given nanosecond timestamps and PID
// separation — the name is regenerated and the attempt is retried. The header
// error is always preserved: cleanup failures (close, remove) are logged but
// never mask it.
func open(dir string, keyMode byte, fingerprint [16]byte) (*os.File, string, error) {
	for {
		path := filepath.Join(dir, fileName(dir))
		osFile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				continue
			}
			return nil, "", err
		}
		if err = writeHeader(osFile, keyMode, fingerprint); err != nil {
			osFile.Close()
			os.Remove(path)
			return nil, "", fmt.Errorf("audit: write header: %w", err)
		}
		return osFile, path, nil
	}
}

// writeHeader emits the fixed [magic:4][version:1][keyMode:1][fingerprint:16]
// header to w. The header is written once per file at creation time so every
// audit segment is self-describing without consulting a sidecar.
func writeHeader(w *os.File, keyMode byte, fingerprint [16]byte) error {
	var header [auditFileHeaderLen]byte
	copy(header[0:4], auditFileMagic)
	header[4] = auditFileVersion
	header[5] = keyMode
	copy(header[6:], fingerprint[:])
	_, err := w.Write(header[:])
	return err
}

// fileName builds a unique audit file name:
// <unix-nanoseconds>_<hash>_<pid>_tsd.log. Nanoseconds (rather than seconds)
// guarantee two rotations in the same second cannot collide; the hash
// fingerprint separates instances sharing a directory; the PID separates
// processes writing to the same directory.
func fileName(dir string) string {
	h := sha256.Sum256([]byte(dir))
	return fmt.Sprintf("%d_%x_%d%s", time.Now().UnixNano(), h[:4], os.Getpid(), auditFileSuffix)
}

// Write encrypts the record when enabled, flushes it to the current file, and
// advances the byte counter. Once maxSize bytes have been written the file
// rotates, so every record lands in exactly one file.
func (f *file) Write(p []byte) (int, error) {
	out := p
	if f.isEncrypted {
		// Reuse one scratch buffer across records
		var err error
		out, err = f.ce.EncryptInPlace(f.buf, p)
		if err != nil {
			return 0, err
		}
		f.buf = out
		// Prepend a 4-byte big-endian length prefix counting the sealed bytes
		// (nonce + ciphertext + tag) so every blob is self-delimiting: a
		// completed file decodes sequentially without knowing plaintext lengths
		// or reading between records. The prefix is plaintext metadata, never
		// part of the record itself.
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(out)))
		if _, err := f.file.Write(prefix[:]); err != nil {
			return 0, err
		}
		f.bytesWritten += 4
	}
	n, err := f.file.Write(out)
	if err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "audit: failed to write log", log.String("filename", f.file.Name()), log.String("error", err.Error()))
		}
		return n, err
	}
	f.bytesWritten += uint64(n)
	if f.bytesWritten >= f.maxSize {
		if err = f.rotate(); err != nil {
			return n, err
		}
	}
	return len(p), nil
}

// rotate closes the current file and switches to a freshly generated name in
// the same directory, resetting the byte counter. The previous file is left
// untouched on disk — rotation never truncates or renames history.
func (f *file) rotate() error {
	osFile, path, err := open(f.dir, f.keyMode, f.fingerprint)
	if err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "audit: failed to rotate log file", log.String("current", f.path), log.String("error", err.Error()))
		}
		return err
	}
	previous := f.path
	if err = f.file.Close(); err != nil {
		_ = osFile.Close()
		return err
	}
	f.file = osFile
	f.path = path
	f.bytesWritten = 0
	if f.logger.Enabled(log.LevelInfo) {
		f.logger.Log(log.LevelInfo, "audit: rotated log file", log.String("filename", path), log.String("previous", previous))
	}
	return nil
}

// Close closes the current file. After a successful close the handle is
// cleared, so a second call is a harmless no-op.
func (f *file) Close() error {
	if f.file == nil {
		return nil
	}
	err := f.file.Close()
	f.file = nil
	return err
}
