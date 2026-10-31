//go:build !linux || !io_uring

package iouring

import "errors"

// Server is the io_uring echo server. On builds without the io_uring tag (or on
// non-Linux), Listen always fails with ErrUnsupported so callers fall back to
// the gnet backend.
type Server struct{}

var ErrUnsupported = errors.New("iouring: not built with io_uring support")

func Listen(addr string) (*Server, error) { return nil, ErrUnsupported }
func (s *Server) Serve() error            { return ErrUnsupported }
func (s *Server) Stop()                   {}
func (s *Server) Addr() string            { return "" }
