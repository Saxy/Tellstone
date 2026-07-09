/*
Package log
Tellstone Cloud-Native In-Memory Database
File: shard_logger.go
Description: Per-shard logging wrapper that injects the shard ID into every log line so operators can correlate messages with a specific shard.

Authors:

	Maximilian Hagen
*/
package log

type ShardLogger struct {
	base    Logger
	shardID uint64
}

func NewShardLogger(base Logger, shardID uint32) *ShardLogger {
	return &ShardLogger{base: base, shardID: uint64(shardID)}
}

func (sl *ShardLogger) Enabled(level Level) bool {
	return sl.base.Enabled(level)
}

func (sl *ShardLogger) Log(level Level, msg string, fields ...Field) {
	fields = append(fields, Uint64("shard_id", sl.shardID))
	sl.base.Log(level, msg, fields...)
}
