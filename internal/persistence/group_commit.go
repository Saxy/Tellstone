package persistence

import (
	"errors"
	"sync"
	"time"
)

type commitReq struct {
	data []byte
	done chan error
}

type groupCommitter struct {
	h        *shardHandle
	stg      *Storage
	ch       chan *commitReq
	stopCh   chan struct{}
	wg       sync.WaitGroup
	batchSz  int
	timeout  time.Duration
	useFsync bool
}

func newGroupCommitter(stg *Storage, h *shardHandle, batchSz int, timeout time.Duration, useFsync bool) *groupCommitter {
	if batchSz <= 0 {
		batchSz = 128
	}
	if timeout <= 0 {
		timeout = 500 * time.Microsecond
	}
	gc := &groupCommitter{
		h:        h,
		stg:      stg,
		ch:       make(chan *commitReq, batchSz*4),
		stopCh:   make(chan struct{}),
		batchSz:  batchSz,
		timeout:  timeout,
		useFsync: useFsync,
	}
	gc.wg.Add(1)
	go gc.run()
	return gc
}

func (gc *groupCommitter) run() {
	defer gc.wg.Done()
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
				if gc.useFsync {
					writeErr = gc.h.file.Sync()
				} else {
					if err := runtimeFSync(gc.h.file); err != nil {
						writeErr = gc.h.file.Sync()
					}
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
					for _, req := range batch {
						if req.done != nil {
							req.done <- errors.New("persistence: closed")
							close(req.done)
						}
					}
					return
				}
			}
		case req := <-gc.ch:
			batch = append(batch, req)
			if len(batch) >= gc.batchSz {
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

func (gc *groupCommitter) enqueue(data []byte) error {
	done := make(chan error, 1)
	req := &commitReq{data: data, done: done}
	select {
	case gc.ch <- req:
	case <-gc.stopCh:
		return errors.New("persistence: closed")
	}
	err := <-done
	return err
}

func (gc *groupCommitter) flush() error {
	done := make(chan error, 1)
	req := &commitReq{data: nil, done: done}
	select {
	case gc.ch <- req:
	case <-gc.stopCh:
		return errors.New("persistence: closed")
	}
	return <-done
}

func (gc *groupCommitter) stop() {
	close(gc.stopCh)
	gc.wg.Wait()
}
