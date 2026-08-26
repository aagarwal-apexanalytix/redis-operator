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

// TestCommitReplacesFileItCannotOpenForWriting is the regression test for the wedge that took 50
// Sentinel pods across 40 namespaces offline on 2026-08-26.
//
// After the first successful start the config on disk is no longer the file the init container
// wrote: Sentinel rewrites its own config at runtime and the result is owned by the SENTINEL
// runtime user. The init container runs as a different (non-root) user, so a Commit that opens the
// target O_TRUNC gets "permission denied" and the pod never starts again — permanently, because an
// emptyDir survives sandbox recreation and a StatefulSet cannot progress past a non-Ready pod-0.
//
// A read-only existing file reproduces the same "cannot open the existing target for writing"
// condition portably. This test FAILS against the previous os.WriteFile implementation.
func TestCommitReplacesFileItCannotOpenForWriting(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: DAC permission checks are bypassed, so this condition cannot be reproduced")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "sentinel.conf")

	// Stand in for the file Sentinel rewrote: present, and not writable by us.
	if err := os.WriteFile(path, []byte("stale config from a previous run\n"), 0o444); err != nil {
		t.Fatalf("seeding read-only config failed: %v", err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatalf("chmod 0444 failed: %v", err)
	}

	cfg := NewConfig(path, "# generated")
	cfg.Append("sentinel monitor", "mymaster", "10.0.0.1", "6379", "2")
	if err := cfg.Commit(); err != nil {
		t.Fatalf("Commit must replace a config it cannot open for writing, got: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if strings.Contains(string(got), "stale config from a previous run") {
		t.Errorf("stale content survived Commit; rendered config:\n%s", got)
	}
	if !strings.Contains(string(got), "sentinel monitor mymaster 10.0.0.1 6379 2") {
		t.Errorf("freshly generated directive missing; rendered config:\n%s", got)
	}
}

// TestCommitReplacesByRenameNotTruncate proves the mechanism rather than one symptom of it, and
// unlike the test above it holds even when the suite runs as root: a rename installs a NEW inode,
// an in-place truncate reuses the existing one. Renaming is what makes Commit independent of who
// owns the file already there — it needs only write+execute on the directory.
func TestCommitReplacesByRenameNotTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sentinel.conf")

	if err := os.WriteFile(path, []byte("previous\n"), 0o644); err != nil {
		t.Fatalf("seeding config failed: %v", err)
	}
	var before syscall.Stat_t
	if err := syscall.Stat(path, &before); err != nil {
		t.Fatalf("stat before failed: %v", err)
	}

	cfg := NewConfig(path, "# generated")
	if err := cfg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	var after syscall.Stat_t
	if err := syscall.Stat(path, &after); err != nil {
		t.Fatalf("stat after failed: %v", err)
	}
	if before.Ino == after.Ino {
		t.Errorf("config was truncated in place (inode %d unchanged); Commit must rename a new file over the target", after.Ino)
	}
}

// TestCommitLeavesNoTempFiles guards the rename's own failure mode: a temp file abandoned in the
// config directory. Sentinel's config directory is also its working directory, so litter there is
// not harmless.
func TestCommitLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sentinel.conf")

	cfg := NewConfig(path, "# generated")
	if err := cfg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "sentinel.conf" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("expected only sentinel.conf in the config dir, got %v", names)
	}
}
