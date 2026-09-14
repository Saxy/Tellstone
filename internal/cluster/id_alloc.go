/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: id_alloc.go
Description: Globally unique region-ID allocation for Phase 4 region splitting.
The allocator CAS-advances a persistent counter in etcd so concurrent PD
coordinators (or a restarted one) never hand out a duplicate region ID. Derived
region IDs are drawn exclusively from this counter; the bootstrap region (ID 1)
is the seed value.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// regionIDCounterKey persists the highest allocated region ID. Its etcd
// ModRevision drives the compare-and-swap allocator.
const regionIDCounterKey = "/tellstone/region-id-counter"

// ErrRegionIDExhausted is returned when the counter reaches math.MaxUint64
// (practically unreachable; kept for symmetry with the TSO granter).
var ErrRegionIDExhausted = errors.New("cluster: region ID space exhausted (max uint64)")

// RegionIDAllocator hands out globally unique region IDs via a CAS loop on a
// persistent etcd counter.
type RegionIDAllocator struct {
	cli *clientv3.Client
}

// NewRegionIDAllocator wraps an established etcd client.
func NewRegionIDAllocator(cli *clientv3.Client) *RegionIDAllocator {
	return &RegionIDAllocator{cli: cli}
}

// NextID reserves and returns the next region ID. On CAS conflict it re-reads
// the counter and retries until the context is cancelled.
func (a *RegionIDAllocator) NextID(ctx context.Context) (uint64, error) {
	for {
		getResp, err := a.cli.Get(ctx, regionIDCounterKey)
		if err != nil {
			return 0, fmt.Errorf("cluster: reading region ID counter: %w", err)
		}
		counter := uint64(1) // the bootstrap region is ID 1
		rev := int64(0)
		if len(getResp.Kvs) == 1 {
			if len(getResp.Kvs[0].Value) != 8 {
				return 0, fmt.Errorf("cluster: malformed region ID counter value (len %d, want 8)", len(getResp.Kvs[0].Value))
			}
			counter = binary.BigEndian.Uint64(getResp.Kvs[0].Value)
			rev = getResp.Kvs[0].ModRevision
		}
		if counter == ^uint64(0) {
			return 0, ErrRegionIDExhausted
		}
		next := counter + 1
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, next)
		txn, err := a.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(regionIDCounterKey), "=", rev)).
			Then(clientv3.OpPut(regionIDCounterKey, string(buf))).
			Commit()
		if err != nil {
			return 0, fmt.Errorf("cluster: committing region ID counter: %w", err)
		}
		if txn.Succeeded {
			return next, nil
		}
	}
}

// SeedFromRegions advances the counter to at least the highest existing region
// ID. This recovers the allocator if the counter key was lost or compacted
// while region metadata survived. Safe to call on startup.
func (a *RegionIDAllocator) SeedFromRegions(ctx context.Context) error {
	resp, err := a.cli.Get(ctx, regionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("cluster: reading regions for ID seed: %w", err)
	}
	maxID := uint64(1)
	for _, kv := range resp.Kvs {
		if r, ok := decodeRegion(kv.Value); ok && r.ID > maxID {
			maxID = r.ID
		}
	}
	// CAS to maxID so every concurrent allocator observes the floor.
	for {
		getResp, err := a.cli.Get(ctx, regionIDCounterKey)
		if err != nil {
			return fmt.Errorf("cluster: reading region ID counter: %w", err)
		}
		counter := uint64(1)
		rev := int64(0)
		if len(getResp.Kvs) == 1 {
			if len(getResp.Kvs[0].Value) != 8 {
				return fmt.Errorf("cluster: malformed region ID counter value (len %d, want 8)", len(getResp.Kvs[0].Value))
			}
			counter = binary.BigEndian.Uint64(getResp.Kvs[0].Value)
			rev = getResp.Kvs[0].ModRevision
		}
		if counter >= maxID {
			return nil
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, maxID)
		txn, err := a.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(regionIDCounterKey), "=", rev)).
			Then(clientv3.OpPut(regionIDCounterKey, string(buf))).
			Commit()
		if err != nil {
			return fmt.Errorf("cluster: committing region ID seed: %w", err)
		}
		if txn.Succeeded {
			return nil
		}
	}
}
