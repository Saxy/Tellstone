/*
Package shard
Tellstone Shared-Nothing Shard Layer
File: shard.go
Description: Defines the core types for the shared-nothing (SN) architecture. Each shard owns an independent storage engine and is accessed synchronously through Execute(). The shard count is configured via --shards and defaults to the number of CPU cores.

Authors:

	Maximilian Hagen
*/
package shard

import "errors"

var ErrShardStopped = errors.New("shard: stopped")

type ID uint32

type Response struct {
	Value []byte
	OK    bool
	Err   error
}
