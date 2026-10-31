package shard

// CacheLineSize is the typical cache line size (64 bytes on most systems).
const CacheLineSize = 64

// AlignToCacheLine rounds size up to a multiple of the cache line size.
func AlignToCacheLine(size int) int {
	return (size + CacheLineSize - 1) & ^(CacheLineSize - 1)
}

// cacheLinePad separates fields that are written by different goroutines so
// that a write to one does not invalidate the cache line holding the other.
type cacheLinePad [CacheLineSize]byte

// shardCounters are updated from connection goroutines. Each counter sits on
// its own cache line so that connection accounting does not bounce a line that
// the shard's request path reads.
//
//go:align 64
type shardCounters struct {
	connectedClients int64
	_                cacheLinePad
	totalConnections uint64
	_                cacheLinePad
	bytesRead        uint64
	_                cacheLinePad
	bytesWritten     uint64
	_                cacheLinePad
}
