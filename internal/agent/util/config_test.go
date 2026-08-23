package util

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestAppend_RejectsNewlineInjection guards against CRD-driven Sentinel
// config injection (issue #1763): a malicious masterGroupName that contains
// newlines must not be allowed to introduce extra config directives into
// sentinel.conf.
func TestAppend_RejectsNewlineInjection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	cfg := NewConfig(path)

	// Mimics the agent's call site at internal/agent/bootstrap/sentinel/config.go
	// with the PoC payload from issue #1763.
	maliciousGroupName := "mymaster 127.0.0.1 6379 2\nsentinel deny-scripts-reconfig no\nsentinel set-auth-pass mymaster injected-password"
	cfg.Append("sentinel monitor", maliciousGroupName, "127.0.0.1", "6379", "2")

	if err := cfg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	got := string(contents)

	// The injection vector is newlines inside an appended value: each new
	// line in sentinel.conf is parsed as an independent directive. Reject
	// any directive line attacker-controlled tokens could spawn.
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "sentinel deny-scripts-reconfig") ||
			strings.HasPrefix(line, "sentinel set-auth-pass") {
			t.Errorf("config contains injected directive line %q; rendered config:\n%s", line, got)
		}
	}
	if strings.Count(got, "\nsentinel monitor") != 1 {
		t.Errorf("expected exactly one 'sentinel monitor' directive; rendered config:\n%s", got)
	}
}

// TestAppend_PreservesValidValues ensures sanitization does not mangle
// well-formed inputs.
func TestAppend_PreservesValidValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	cfg := NewConfig(path)

	cfg.Append("sentinel monitor", "mymaster", "127.0.0.1", "6379", "2")
	cfg.Append("sentinel down-after-milliseconds", "mymaster", "30000")

	if err := cfg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	got := string(contents)

	for _, want := range []string{
		"sentinel monitor mymaster 127.0.0.1 6379 2",
		"sentinel down-after-milliseconds mymaster 30000",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q in rendered config, got:\n%s", want, got)
		}
	}
}

// TestCommitWritableByRuntimeUser pins the file mode of the generated config.
//
// Redis and Sentinel rewrite their own config at runtime. The init container that generates it
// runs as root; the main container does not. A root-owned read-only config makes Sentinel refuse
// to start outright:
//
//	Sentinel config file /etc/redis/sentinel.conf is not writable: Permission denied. Exiting...
//
// Two things make this easy to reintroduce, and the test covers both:
//   - os.WriteFile's perm is masked by the process UMASK, so passing 0666 lands as 0644 under a
//     typical 022 — the mode must be set explicitly;
//   - os.WriteFile ignores perm ENTIRELY when the file already exists, which is the case on every
//     restart after the first.
func TestCommitWritableByRuntimeUser(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sentinel.conf")

	// Force a umask that would strip group/other write from a 0666 create. If Commit relies on
	// os.WriteFile's perm alone, this is what silently reintroduces the crashloop.
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)

	// (1) fresh file
	if err := NewConfig(path, "port 26379").Commit(); err != nil {
		t.Fatalf("Commit (create) failed: %v", err)
	}
	assertWritableByNonOwner(t, path, "freshly created config")

	// (2) file already exists — the restart case, where WriteFile's perm is ignored
	if err := os.Chmod(path, 0o600); err != nil { // simulate a pre-existing restrictive mode
		t.Fatalf("chmod: %v", err)
	}
	if err := NewConfig(path, "port 26379\nmaxclients 100").Commit(); err != nil {
		t.Fatalf("Commit (overwrite) failed: %v", err)
	}
	assertWritableByNonOwner(t, path, "config overwritten on restart")

	// content must still be correct
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(b), "maxclients 100") {
		t.Errorf("Commit did not write the new content: %q", string(b))
	}
}

func assertWritableByNonOwner(t *testing.T, path, what string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	mode := fi.Mode().Perm()
	// group AND other write — the main container may run as an arbitrary uid/gid
	if mode&0o020 == 0 || mode&0o002 == 0 {
		t.Errorf("%s has mode %#o; it must be writable by the non-root runtime user "+
			"(need group+other write) or Sentinel exits with "+
			"\"config file ... is not writable: Permission denied\"", what, mode)
	}
}
