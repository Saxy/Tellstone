/*
Package rbac
Tellstone Role-Based Access Control
File: rbac.go
Description: Dynamic command-permission bitset.
Pre-sized at policy load time, so the authorization hot path is a single bit test with zero allocations.

Authors:

	Maximilian Hagen
*/
package rbac

// Bitset maps command IDs to a dense bit array (word = id/64, offset = id%64).
// It grows in 64-bit words as commands are registered and is pre-allocated at
// policy load time, so the hot-path Has checked never allocates. The zero value
// is deny-all (fail closed).
type Bitset []uint64

// Set marks id as granted, growing the slice in 64-bit words if needed. Growth
// happens only at policy build time, never on the request path.
func (b *Bitset) Set(id uint16) {
	word, bit := id/64, uint64(1)<<(id%64)
	if int(word) >= len(*b) {
		*b = append(*b, make([]uint64, int(word)-len(*b)+1)...)
	}
	(*b)[word] |= bit
}

// Has reports whether id is granted. Out-of-range IDs are denied. The
// authorization hot path is a single dereference plus bit test with zero
// allocations.
func (b *Bitset) Has(id uint16) bool {
	word := id / 64
	if int(word) >= len(*b) {
		return false
	}
	v := *b
	return v[word]&(uint64(1)<<(id%64)) != 0
}

// Clear revokes id. Out-of-range IDs are a no-op. It mutates the existing
// backing array in place, so it never reallocates or grows the slice.
func (b *Bitset) Clear(id uint16) {
	word := id / 64
	if int(word) >= len(*b) {
		return
	}
	v := *b
	v[word] &^= uint64(1) << (id % 64)
}

// NewBitset returns a bitset pre-sized for commands with every listed ID granted.
func NewBitset(commands []uint16) Bitset {
	b := make(Bitset, maxCommand(commands)/64+1)
	for _, id := range commands {
		b.Set(id)
	}
	return b
}

// maxCommand returns the largest command ID in commands, or 0 for an empty list.
func maxCommand(commands []uint16) uint16 {
	var maxCmd uint16
	for _, id := range commands {
		if id > maxCmd {
			maxCmd = id
		}
	}
	return maxCmd
}
