package persistence

import (
	"errors"
	"sync"
	"time"
)

type commitReq struct {
	data     []byte
	done     chan error
	flushNow bool
}

type groupCommitter struct {
	h            *shardHandle
	stg          *Storage
	ch           chan *commitReq
	stopCh       chan struct{}
	stopOnce     sync.Once
	exited       chan struct{}
	wg           sync.WaitGroup
	batchSz      int
	timeout      time.Duration
	useFdatasync bool
}

func newGroupCommitter(stg *Storage, h *shardHandle, batchSz int, timeout time.Duration, useFdatasync bool) *groupCommitter {
	if batchSz <= 0 {
		batchSz = 128
	}
	if timeout <= 0 {
		timeout = 500 * time.Microsecond
	}
	gc := &groupCommitter{
		h:            h,
		stg:          stg,
		ch:           make(chan *commitReq, batchSz*4),
		stopCh:       make(chan struct{}),
		exited:       make(chan struct{}),
		batchSz:      batchSz,
		timeout:      timeout,
		useFdatasync: useFdatasync,
	}
	gc.wg.Add(1)
	go gc.run()
	return gc
}

func (gc *groupCommitter) run() {
	defer gc.wg.Done()
	defer close(gc.exited)
	batch := make([]*commitReq, 0, gc.batchSz)
	timer := time.NewTimer(gc.timeout)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		var writeErr error
		if gc.h.needsMigrate {
			writeErr = errors.New("persistence: shard migration in progress")
		} else {
			for _, req := range batch {
				if len(req.data) == 0 {
					continue
				}
				if _, err := gc.h.file.Write(req.data); err != nil {
					writeErr = err
					break
				}
			}
			if writeErr == nil {
				if gc.useFdatasync {
					writeErr = runtimeFSync(gc.h.file)
				} else {
					writeErr = gc.h.file.Sync()
				}
			}
		}
		for _, req := range batch {
			if req.done != nil {
				req.done <- writeErr
				close(req.done)
			}
		}
		batch = batch[:0]
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(gc.timeout)
	}

	for {
		select {
		case <-gc.stopCh:
			for {
				select {
				case req := <-gc.ch:
					batch = append(batch, req)
					if len(batch) >= gc.batchSz {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case req := <-gc.ch:
			batch = append(batch, req)
			// Flush on a full batch, or immediately when this request is the
			// only one queued and asked for it. The second condition is what
			// lets a lone transaction skip the group-commit window entirely
			// without costing concurrency: as soon as another writer is
			// waiting, the window is kept so the two can share one flush.
			if len(batch) >= gc.batchSz || (req.flushNow && len(gc.ch) == 0) {
				flush()
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(gc.timeout)
		case <-timer.C:
			flush()
		}
	}
}

func (gc *groupCommitter) enqueue(data []byte, flushNow bool) error {
	done := make(chan error, 1)
	req := &commitReq{data: data, done: done, flushNow: flushNow}
	select {
	case gc.ch <- req:
	case <-gc.stopCh:
		select {
		case <-gc.exited:
		default:
		}
		return errors.New("persistence: closed")
	case <-gc.exited:
		return errors.New("persistence: closed")
	}
	select {
	case err := <-done:
		return err
	case <-gc.exited:
		return errors.New("persistence: closed")
	}
}

func (gc *groupCommitter) flush() error {
	done := make(chan error, 1)
	req := &commitReq{data: nil, done: done}
	select {
	case gc.ch <- req:
	case <-gc.stopCh:
		return errors.New("persistence: closed")
	case <-gc.exited:
		return errors.New("persistence: closed")
	}
	select {
	case err := <-done:
		return err
	case <-gc.exited:
		return errors.New("persistence: closed")
	}
}

func (gc *groupCommitter) stop() {
	gc.stopOnce.Do(func() {
		close(gc.stopCh)
	})
	gc.wg.Wait()
}
