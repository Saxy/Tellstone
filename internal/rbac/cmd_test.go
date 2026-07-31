/*
Package rbac
Tellstone Role-Based Access Control Tests
File: cmd_test.go
Description: Verifies the command registries stay in sync: every ID in AllCommands is
reachable by name, and the name map holds no extra or dangling entries.
*/
package rbac

import "testing"

// TestCommandRegistriesStayInSync guards against a common drift when a new CmdXxx
// constant is added: the AllCommands list and the commandNames map must both be
// updated, otherwise LookupCommand fails for a command that still has an ID.
func TestCommandRegistriesStayInSync(t *testing.T) {
	if len(commandNames) != len(AllCommands) {
		t.Fatalf("commandNames has %d entries, AllCommands has %d — they must match",
			len(commandNames), len(AllCommands))
	}
	seen := make(map[uint16]string, len(commandNames))
	for name, id := range commandNames {
		if prev, dup := seen[id]; dup {
			t.Fatalf("command ID %d is mapped by both %q and %q", id, prev, name)
		}
		seen[id] = name
	}
	for _, id := range AllCommands {
		if _, ok := seen[id]; !ok {
			t.Fatalf("command ID %d (%s) is missing from commandNames", id, CommandName(id))
		}
	}
}
