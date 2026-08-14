/*
Package rbac
Tellstone Role-Based Access Control Tests
File: rbac_test.go
Description: Verifies command-category grants and dynamic bitset semantics, including fail-closed behavior and word-boundary growth.
*/
package rbac

import (
	"slices"
	"testing"
)

func TestCategoryReadWriteAllowsLogin(t *testing.T) {
	for _, cat := range []string{"readwrite", "operator"} {
		if !slices.Contains(Category(cat), CmdAuth) {
			t.Errorf("%s category must include CmdAuth so the role can log in", cat)
		}
	}
}

func TestCategoryAllCoversEveryRegisteredCommand(t *testing.T) {
	got := Category("all")
	if len(got) != len(AllCommands) {
		t.Fatalf("all category has %d commands, want %d", len(got), len(AllCommands))
	}
	for _, id := range AllCommands {
		if !slices.Contains(got, id) {
			t.Errorf("all category missing command %d", id)
		}
	}
}

func TestCategoryUnknownReturnsNil(t *testing.T) {
	if got := Category("bogus"); got != nil {
		t.Fatalf("unknown category should be nil, got %v", got)
	}
}

func TestCategoriesForCommandDerivesFromCatalog(t *testing.T) {
	read := CategoriesForCommand(CmdGet)
	if !slices.Contains(read, "read") {
		t.Errorf("CmdGet missing the read category, got %v", read)
	}
	if !slices.Contains(read, "all") {
		t.Errorf("CmdGet missing the implicit all category, got %v", read)
	}
	for i := 1; i < len(read); i++ {
		if read[i-1] >= read[i] {
			t.Errorf("categories not sorted: %v", read)
		}
	}
	if got := CategoriesForCommand(CmdGet); !slices.Equal(got, read) {
		t.Errorf("CategoriesForCommand must be deterministic, got %v", got)
	}
	if got := CategoriesForCommand(0); got != nil {
		t.Errorf("unregistered command id should yield nil, got %v", got)
	}
}

func TestBitsetSetHasRoundTrip(t *testing.T) {
	b := NewBitset(nil)
	for _, id := range AllCommands {
		b.Set(id)
	}
	for _, id := range AllCommands {
		if !b.Has(id) {
			t.Errorf("expected command %d to be granted", id)
		}
	}
}

func TestBitsetGrowsAcrossWords(t *testing.T) {
	var b Bitset
	const high = uint16(129) // word 2, offset 1
	b.Set(high)
	if !b.Has(high) {
		t.Fatal("expected high command ID to be granted after growth")
	}
	if b.Has(high - 1) {
		t.Fatal("unset ID must not be granted")
	}
}

func TestBitsetFailClosed(t *testing.T) {
	var b Bitset
	if b.Has(CmdGet) {
		t.Fatal("zero-value bitset must deny every command")
	}
	b.Set(CmdSet)
	if b.Has(CmdGet) {
		t.Fatal("unset command must be denied")
	}
	if b.Has(20000) {
		t.Fatal("out-of-range command must be denied")
	}
}

func TestNewBitsetPreSizedFromCategory(t *testing.T) {
	read := Category("read")
	b := NewBitset(read)
	for _, id := range read {
		if !b.Has(id) {
			t.Errorf("command %d should be granted by read category", id)
		}
	}
	if b.Has(CmdSet) {
		t.Error("read category must not grant write commands")
	}
}
