package shard

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// maxBatch caps how many queued requests the actor drains before it yields to
// the next select. Requests within a batch run in arrival order.
const maxBatch = 64

// actor serializes every mutation of one shard on a single goroutine. A caller
// submits a request and blocks for its reply, so the observable semantics match
// direct execution while the engine is only ever written from one goroutine.
// Reads do not pass through the actor.
type actor struct {
	queue chan *request
	done  chan struct{}
	wg    sync.WaitGroup
	apply func(op, key string, value []byte, ttl time.Duration) Response
	// pinCore is the CPU the actor goroutine pins itself to, or -1 for none.
	pinCore  int
	onPinErr func(error)

	_        [CacheLineSize]byte
	inflight atomic.Int64
	_        [CacheLineSize]byte
	closed   atomic.Bool
	_        [CacheLineSize]byte
}

// request is one queued mutation. Requests are pooled so a steady-state write
// allocates nothing for its hand-off.
type request struct {
	op    string
	key   string
	value []byte
	ttl   time.Duration
	reply chan Response
}

var requestPool = sync.Pool{New: func() any {
	return &request{reply: make(chan Response, 1)}
}}

func newActor(apply func(op, key string, value []byte, ttl time.Duration) Response, pinCore int, onPinErr func(error)) *actor {
	a := &actor{
		queue:    make(chan *request, 1024),
		done:     make(chan struct{}),
		apply:    apply,
		pinCore:  pinCore,
		onPinErr: onPinErr,
	}
	a.wg.Add(1)
	go a.run()
	return a
}

func (a *actor) run() {
	defer a.wg.Done()
	if a.pinCore >= 0 {
		cp := NewCorePinner(a.pinCore)
		if err := cp.Pin(); err != nil {
			if a.onPinErr != nil {
				a.onPinErr(err)
			}
		} else {
			defer cp.Unpin()
		}
	}
	batch := make([]*request, 0, maxBatch)
	for {
		select {
		case req := <-a.queue:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < maxBatch {
				select {
				case next := <-a.queue:
					batch = append(batch, next)
				default:
					break drain
				}
			}
			for _, r := range batch {
				r.reply <- a.apply(r.op, r.key, r.value, r.ttl)
			}
		case <-a.done:
			return
		}
	}
}

// submit hands one mutation to the actor and waits for its reply. The inflight
// count lets stop wait for every accepted request before the actor exits, so a
// request is never left unanswered.
func (a *actor) submit(op, key string, value []byte, ttl time.Duration) Response {
	a.inflight.Add(1)
	if a.closed.Load() {
		a.inflight.Add(-1)
		return Response{Err: ErrShardStopped}
	}
	req := requestPool.Get().(*request)
	req.op, req.key, req.value, req.ttl = op, key, value, ttl
	a.queue <- req
	resp := <-req.reply
	req.op, req.key, req.value = "", "", nil
	requestPool.Put(req)
	a.inflight.Add(-1)
	return resp
}

// stop rejects new submissions, waits for accepted ones to finish, and then
// terminates the actor goroutine. It is safe to call more than once.
func (a *actor) stop() {
	if a.closed.Swap(true) {
		return
	}
	for a.inflight.Load() != 0 {
		runtime.Gosched()
	}
	close(a.done)
	a.wg.Wait()
}
