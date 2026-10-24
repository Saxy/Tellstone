/*
Package config
Tellstone Cloud-Native In-Memory Database
File: bytesize.go
Description: Utilities for parsing human‑readable byte size strings (e.g., "16MiB", "1GiB"). Implements flag.Value.

Authors:

	Maximilian Hagen
*/
package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ByteSize represents a size in bytes but can be parsed from human‑readable strings
// such as "16MiB", "1GiB", "256KB", etc. It implements flag.Value so it can be used
// directly with the standard flag package. The underlying type is uint64 so memory
// ceilings above 4GiB are representable.
type ByteSize uint64

// String returns the size as a plain integer (bytes). The flag package uses this for
// printing default values in the usage output.
func (b *ByteSize) String() string {
	if *b == 0 {
		return fmt.Sprintf("%d MiB", 16)
	}
	v := uint64(*b)
	const (
		GiB = 1 << 30
		MiB = 1 << 20
		KiB = 1 << 10
	)
	switch {
	case v >= GiB && v%GiB == 0:
		return fmt.Sprintf("%d GiB", v/GiB)
	case v >= MiB && v%MiB == 0:
		return fmt.Sprintf("%d MiB", v/MiB)
	case v >= KiB && v%KiB == 0:
		return fmt.Sprintf("%d KiB", v/KiB)
	default:
		return fmt.Sprintf("%d", v)
	}
}

// Set parses a string with optional unit suffixes. Supported suffixes are:
//
//	KiB, MiB, GiB – binary (powers of 1024)
//	KB, MB, GB – decimal (powers of 1000)
//
// If no suffix is present, the value is interpreted as raw bytes.
func (b *ByteSize) Set(s string) error {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return fmt.Errorf("empty size value")
	}
	var mul uint64 = 1
	switch {
	case strings.HasSuffix(s, "KIB"):
		mul = 1 << 10
		s = strings.TrimSuffix(s, "KIB")
	case strings.HasSuffix(s, "MIB"):
		mul = 1 << 20
		s = strings.TrimSuffix(s, "MIB")
	case strings.HasSuffix(s, "GIB"):
		mul = 1 << 30
		s = strings.TrimSuffix(s, "GIB")
	case strings.HasSuffix(s, "KB"):
		mul = 1000
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "MB"):
		mul = 1000 * 1000
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "GB"):
		mul = 1000 * 1000 * 1000
		s = strings.TrimSuffix(s, "GB")
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid size %q: %w", s, err)
	}
	if mul > 1 && v > math.MaxUint64/mul {
		return fmt.Errorf("size %q overflows uint64", s)
	}
	v *= mul
	*b = ByteSize(v)
	return nil
}
