//go:build linux && io_uring

// Package iouring is a minimal io_uring-backed echo server. It is the prototype
// for Phase 3's io_uring backend: it drives accept, recv and send through one
// io_uring ring over raw syscalls (x/sys/unix exposes the syscall numbers but
// not the io_uring structures, so the SQE/CQE layouts and opcodes are defined
// here). Full integration into internal/network.Server is a follow-up.
package iouring

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Opcodes (linux/io_uring.h).
const (
	opNop    uint8 = 0
	opAccept uint8 = 13
	opSend   uint8 = 26
	opRecv   uint8 = 27
)

// Ring-map offsets passed to mmap.
const (
	offSQRing = 0
	offCQRing = 0x8000000
	offSQEs   = 0x10000000
)

// io_uring_setup/enter flags.
const enterGetEvents = 1

const sqeSize = 64
const cqeSize = 16

type sqringOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	userAddr                                                        uint64
}

type cqringOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	userAddr                                                        uint64
}

type iouringParams struct {
	sqEntries, cqEntries, flags, sqThreadCPU, sqThreadIdle, features, wqFd uint32
	resv                                                                   [3]uint32
	sqOff                                                                  sqringOffsets
	cqOff                                                                  cqringOffsets
}

// ioUringSQE mirrors struct io_uring_sqe. Only the fields the echo path uses are
// named; the tail is padding to the fixed 64-byte size.
type ioUringSQE struct {
	Opcode      uint8
	Flags       uint8
	Ioprio      uint16
	Fd          int32
	Off         uint64
	Addr        uint64
	Len         uint32
	MsgFlags    uint32
	UserData    uint64
	BufIndex    uint16
	BufGroup    uint16
	Personality uint16
	_           [2]byte
	_           [16]byte
}

type ioUringCQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

// conn tracks the single in-flight operation for one accepted connection.
type conn struct {
	fd      int32
	buf     []byte
	sendBuf []byte
	sendOff int   // bytes of buf already handed to the kernel
	sendLen int   // total bytes of buf to echo
	pending uint8 // op pending for this fd
}

// Server is an io_uring echo server. Connections are accepted and echoed
// through one ring driven by Serve.
type Server struct {
	ringFd int
	sqRing []byte
	cqRing []byte
	sqes   []byte

	sqHead *uint32
	sqTail *uint32
	sqMask *uint32
	sqArr  unsafe.Pointer
	cqHead *uint32
	cqTail *uint32
	cqMask *uint32
	cqCqes unsafe.Pointer

	ln      int
	conns   map[int32]*conn
	stopped atomic.Bool
	pending int
}

// Listen creates a Server listening on addr. The ring is not set up until Serve.
func Listen(addr string) (*Server, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	ip := tcpAddr.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("iouring: %q is not an IPv4 address", addr)
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		unix.Close(fd)
		return nil, err
	}
	sa := &unix.SockaddrInet4{Port: tcpAddr.Port}
	copy(sa.Addr[:], ip)
	if err := unix.Bind(fd, sa); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if err := unix.Listen(fd, 1024); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return &Server{ln: fd, conns: make(map[int32]*conn)}, nil
}

// setup creates the ring and mmaps its regions.
func (s *Server) setup() error {
	p := iouringParams{}
	fd, _, errno := syscall.Syscall6(unix.SYS_IO_URING_SETUP, uintptr(256), uintptr(unsafe.Pointer(&p)), 0, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	s.ringFd = int(fd)

	sqRingSize := int(p.sqOff.array) + int(p.sqEntries)*4
	cqRingSize := int(p.cqOff.cqes) + int(p.cqEntries)*cqeSize
	sqesSize := int(p.sqEntries) * sqeSize

	var err error
	if s.sqRing, err = unix.Mmap(s.ringFd, offSQRing, sqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		return err
	}
	if s.cqRing, err = unix.Mmap(s.ringFd, offCQRing, cqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		return err
	}
	if s.sqes, err = unix.Mmap(s.ringFd, offSQEs, sqesSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		return err
	}

	u32 := func(b []byte, off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&b[off])) }
	s.sqHead = u32(s.sqRing, p.sqOff.head)
	s.sqTail = u32(s.sqRing, p.sqOff.tail)
	s.sqMask = u32(s.sqRing, p.sqOff.ringMask)
	s.sqArr = unsafe.Pointer(&s.sqRing[p.sqOff.array])
	s.cqHead = u32(s.cqRing, p.cqOff.head)
	s.cqTail = u32(s.cqRing, p.cqOff.tail)
	s.cqMask = u32(s.cqRing, p.cqOff.ringMask)
	s.cqCqes = unsafe.Pointer(&s.cqRing[p.cqOff.cqes])
	return nil
}

// submit enqueues one SQE and returns it to be flushed by enter.
func (s *Server) submit(sqe *ioUringSQE) {
	tail := atomic.LoadUint32(s.sqTail)
	idx := tail & atomic.LoadUint32(s.sqMask)
	dst := (*ioUringSQE)(unsafe.Add(unsafe.Pointer(&s.sqes[0]), uintptr(idx)*sqeSize))
	*dst = *sqe
	*(*uint32)(unsafe.Add(s.sqArr, uintptr(idx)*4)) = idx
	atomic.StoreUint32(s.sqTail, tail+1)
	s.pending++
}

func (s *Server) enter(submitted int) error {
	_, _, errno := syscall.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(s.ringFd), uintptr(submitted), 1, enterGetEvents, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// submitAccept arms the listener for one accept. The peer address is discarded
// (addr and addrlen are NULL), so the SQE carries no pointer into stack memory
// that would be invalid once the kernel runs it asynchronously.
func (s *Server) submitAccept() {
	sqe := ioUringSQE{
		Opcode:   opAccept,
		Fd:       int32(s.ln),
		UserData: uint64(s.ln), // listener fd identifies the accept
	}
	s.submit(&sqe)
}

func (s *Server) submitRecv(c *conn) {
	sqe := ioUringSQE{
		Opcode:   opRecv,
		Fd:       c.fd,
		Addr:     uint64(uintptr(unsafe.Pointer(&c.buf[0]))),
		Len:      uint32(len(c.buf)),
		UserData: uint64(c.fd),
	}
	c.pending = opRecv
	s.submit(&sqe)
}

func (s *Server) submitSend(c *conn) {
	remaining := c.sendLen - c.sendOff
	sqe := ioUringSQE{
		Opcode:   opSend,
		Fd:       c.fd,
		Addr:     uint64(uintptr(unsafe.Pointer(&c.buf[c.sendOff]))),
		Len:      uint32(remaining),
		UserData: uint64(c.fd),
	}
	c.pending = opSend
	s.submit(&sqe)
}

// Serve runs the echo loop until the server is stopped. It is blocking.
func (s *Server) Serve() error {
	if err := s.setup(); err != nil {
		return err
	}
	defer s.close()
	s.submitAccept()
	for !s.stopped.Load() {
		toSubmit := s.pending
		s.pending = 0
		if err := s.enter(toSubmit); err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		s.drainCompletions()
	}
	return nil
}

func (s *Server) drainCompletions() {
	for {
		head := atomic.LoadUint32(s.cqHead)
		tail := atomic.LoadUint32(s.cqTail)
		if head == tail {
			break
		}
		mask := atomic.LoadUint32(s.cqMask)
		cqe := (*ioUringCQE)(unsafe.Add(s.cqCqes, uintptr(head&mask)*cqeSize))
		s.handleCompletion(cqe)
		atomic.StoreUint32(s.cqHead, head+1)
	}
}

func (s *Server) handleCompletion(cqe *ioUringCQE) {
	fd := int32(cqe.UserData)
	if fd == int32(s.ln) {
		// Accept completed: res is the new connection fd, or an error.
		newFd := cqe.Res
		if newFd < 0 {
			s.submitAccept()
			return
		}
		unix.CloseOnExec(int(newFd))
		c := &conn{fd: int32(newFd), buf: make([]byte, 4096)}
		s.conns[c.fd] = c
		s.submitRecv(c)
		s.submitAccept()
		return
	}
	c, ok := s.conns[fd]
	if !ok {
		return
	}
	switch c.pending {
	case opRecv:
		n := cqe.Res
		if n <= 0 {
			// EOF or error: drop the connection.
			unix.Close(int(fd))
			delete(s.conns, fd)
			return
		}
		c.sendOff = 0
		c.sendLen = int(n)
		s.submitSend(c)
	case opSend:
		n := cqe.Res
		if n <= 0 {
			// The peer is gone or the send failed: drop the connection
			// rather than spinning on a buffer the kernel will not accept.
			unix.Close(int(fd))
			delete(s.conns, fd)
			return
		}
		c.sendOff += int(n)
		if c.sendOff < c.sendLen {
			// A short send leaves the rest of the echo in the buffer.
			s.submitSend(c)
			return
		}
		c.sendOff = 0
		c.sendLen = 0
		s.submitRecv(c)
	}
}

func (s *Server) close() {
	unix.Close(s.ln)
	for fd := range s.conns {
		unix.Close(int(fd))
	}
	unix.Munmap(s.sqRing)
	unix.Munmap(s.cqRing)
	unix.Munmap(s.sqes)
	unix.Close(s.ringFd)
}

// Stop signals Serve to return after its current iteration.
func (s *Server) Stop() { s.stopped.Store(true) }

// Addr returns the listener's bound address. The port is the real one only when
// Listen was given an explicit "127.0.0.1:0" address; callers that need the
// assigned port should pass one via ResolveTCPAddr.
func (s *Server) Addr() string {
	sa, err := unix.Getsockname(s.ln)
	if err != nil {
		return ""
	}
	if inet, ok := sa.(*unix.SockaddrInet4); ok {
		return net.JoinHostPort("127.0.0.1", itoa(int(inet.Port)))
	}
	return ""
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
