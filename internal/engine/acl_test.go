package engine

import (
	"strings"
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestMultiUserACLandPermissions(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.SetMasterPassword("master_secret")

	// 1. Unauthenticated client cannot run commands
	res := eng.ExecuteCommand("c1", []string{"GET", "foo"})
	if res.Type != resp.ErrorPrefix || !strings.Contains(res.Str, "NOAUTH") {
		t.Fatalf("Expected NOAUTH, got: %v", res)
	}

	// 2. Authenticate as default admin
	authRes := eng.ExecuteCommand("c1", []string{"AUTH", "master_secret"})
	if authRes.Str != "OK" {
		t.Fatalf("Expected OK from master AUTH, got: %v", authRes)
	}

	// 3. Test ACL WHOAMI
	whoRes := eng.ExecuteCommand("c1", []string{"ACL", "WHOAMI"})
	if whoRes.String() != "default" {
		t.Fatalf("Expected default username, got: %v", whoRes)
	}

	// 4. Create Read-Only User with ACL SETUSER: alice
	// ACL SETUSER alice on >alice_pass +@read ~cache:*
	setUserRes := eng.ExecuteCommand("c1", []string{
		"ACL", "SETUSER", "alice", "on", ">alice_pass", "+@read", "~cache:*",
	})
	if setUserRes.Str != "OK" {
		t.Fatalf("Expected OK from ACL SETUSER, got: %v", setUserRes)
	}

	// 5. Connect new client as alice
	resAliceAuth := eng.ExecuteCommand("c2", []string{"AUTH", "alice", "alice_pass"})
	if resAliceAuth.Str != "OK" {
		t.Fatalf("Expected OK from alice AUTH, got: %v", resAliceAuth)
	}

	whoAlice := eng.ExecuteCommand("c2", []string{"ACL", "WHOAMI"})
	if whoAlice.String() != "alice" {
		t.Fatalf("Expected alice username, got: %v", whoAlice)
	}

	// 6. Alice should be BLOCKED from mutating commands (SET)
	resAliceSet := eng.ExecuteCommand("c2", []string{"SET", "cache:test", "123"})
	if resAliceSet.Type != resp.ErrorPrefix || !strings.Contains(resAliceSet.Str, "NOPERM") {
		t.Fatalf("Expected NOPERM error for read-only user on SET, got: %v", resAliceSet)
	}

	// 7. Alice should be BLOCKED from accessing keys outside ~cache:*
	resAliceGetSecret := eng.ExecuteCommand("c2", []string{"GET", "secret:users"})
	if resAliceGetSecret.Type != resp.ErrorPrefix || !strings.Contains(resAliceGetSecret.Str, "NOPERM") {
		t.Fatalf("Expected NOPERM error for out-of-namespace key, got: %v", resAliceGetSecret)
	}

	// 8. Alice should be PERMITTED on GET inside ~cache:*
	// First let admin set it
	eng.ExecuteCommand("c1", []string{"SET", "cache:item", "hello"})
	resAliceGet := eng.ExecuteCommand("c2", []string{"GET", "cache:item"})
	if resAliceGet.String() != "hello" {
		t.Fatalf("Expected alice to read cache:item, got: %v", resAliceGet)
	}

	// 9. Admin checks ACL USERS
	usersRes := eng.ExecuteCommand("c1", []string{"ACL", "USERS"})
	if usersRes.Type != resp.ArrayPrefix || len(usersRes.Array) < 2 {
		t.Fatalf("Expected at least 2 users in ACL USERS, got: %v", usersRes)
	}

	// 10. Admin deletes user
	delRes := eng.ExecuteCommand("c1", []string{"ACL", "DELUSER", "alice"})
	if delRes.Num != 1 {
		t.Fatalf("Expected 1 deleted user, got: %v", delRes)
	}
}
