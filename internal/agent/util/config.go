package util

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	content string
	path    string
}

func NewConfig(path string, defaultConfig ...string) *Config {
	return &Config{
		path:    path,
		content: strings.Join(defaultConfig, "\n"),
	}
}

// configValueSanitizer strips the line-breaking characters (CR/LF) that would
// let a value escape its directive line. Redis only splits config into
// directives on '\n' and only treats '#' as a comment at the start of a line,
// and these values are always written after a fixed directive token, so
// stripping CR/LF is sufficient to prevent directive injection.
var configValueSanitizer = strings.NewReplacer(
	"\r", "",
	"\n", "",
)

// sanitizeConfigValue removes characters that would let a value escape its
// directive line. Cleaning rather than erroring keeps the bootstrap path
// resilient when upstream CRD validation is bypassed (e.g. existing objects,
// partial RBAC).
func sanitizeConfigValue(s string) string {
	return configValueSanitizer.Replace(s)
}

func (c *Config) Append(config ...string) *Config {
	if len(config) == 0 {
		return c
	}
	directive := sanitizeConfigValue(config[0])
	args := make([]string, 0, len(config)-1)
	for _, a := range config[1:] {
		args = append(args, sanitizeConfigValue(a))
	}
	c.content = fmt.Sprintf("%s\n%s %s", c.content, directive, strings.Join(args, " "))
	return c
}

// configFileMode makes the generated config writable by the RUNTIME user, not just by the
// (root) init container that generates it.
//
// Redis and Sentinel REWRITE their own config file at runtime — Sentinel does so on every
// topology change (CONFIG REWRITE / "Sentinel new configuration saved on disk"). If the file is
// not writable by the user the main container runs as, Sentinel refuses to start at all:
//
//	Sentinel config file /etc/redis/sentinel.conf is not writable: Permission denied. Exiting...
//
// The init container runs as root and the main container does not, so a root-owned 0644 file is
// readable-but-not-writable and every sentinel pod crashloops the moment
// GenerateConfigInInitContainer is enabled. Observed fleet-wide on a 2026-08-23 rollout: the
// StatefulSet halted its rolling update at the first pod, which is the only reason quorum held.
//
// An fsGroup does not solve this on its own: fsGroup ownership is applied to the volume at MOUNT
// time, before the init container writes, and a newly created 0644 file only inherits the GROUP —
// leaving group read-only. The file mode is the binding constraint.
const configFileMode = 0o666

// tempPattern derives the os.CreateTemp pattern from the TARGET file rather than hardcoding a name.
// Commit() is shared by both bootstrap paths — sentinel.conf and redis.conf — and they share
// /etc/redis, so a hardcoded ".sentinel.conf.*" would leave sentinel-named temporaries behind while
// writing redis.conf, and would name them after a file that Commit was never writing if it failed
// midway. Factored out as a function so the naming is directly testable: after a SUCCESSFUL Commit
// the temp has been renamed away, so no end-state assertion on the directory can observe it.
func tempPattern(path string) string {
	return "." + filepath.Base(path) + ".*"
}

func (c *Config) Commit() error {
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	// Write to a temporary file in the SAME directory and rename over the target, rather than
	// truncating the target in place.
	//
	// Why in-place truncation is not safe here: os.WriteFile opens the target O_WRONLY|O_CREATE|
	// O_TRUNC, which requires WRITE permission on the EXISTING FILE. After the first successful
	// start, that file is no longer the one this code wrote — Sentinel REWRITES its own config at
	// runtime (write-temp-then-rename), so the file on disk is owned by the SENTINEL runtime user
	// and carries Sentinel's own 0644. The init container does not run as that user (it runs as
	// the operator image's non-root user), so on the next re-run it gets:
	//
	//	Error: open /etc/redis/sentinel.conf: permission denied
	//
	// and the pod never starts. Init containers re-run whenever the pod SANDBOX is recreated (node
	// reboot, kubelet/containerd restart, eviction) while an emptyDir survives, so this is not a
	// rare path — and because a StatefulSet cannot proceed past a pod that never becomes Ready, one
	// wedged pod takes the whole Sentinel quorum with it. Observed on a 2026-08-23 rollout: 50 pods
	// across 40 namespaces wedged permanently on the first node event, three days later.
	//
	// Chmod-after-write does not help: it fixes the MODE of a file this process could already open,
	// and the failure is that it cannot open it at all. Rename needs only WRITE+EXECUTE on the
	// DIRECTORY, which the config volume grants, so it works regardless of who owns the existing
	// file — and it is atomic, so a reader never sees a partial config.
	tmp, err := os.CreateTemp(dir, tempPattern(c.path))
	if err != nil {
		return fmt.Errorf("failed to create temp config in %s: %v", dir, err)
	}
	tmpName := tmp.Name()
	defer func() {
		// No-op once the rename below has succeeded; cleans up on every error path.
		_ = os.Remove(tmpName)
	}()

	if _, err := tmp.WriteString(c.content); err != nil {
		tmp.Close()
		return fmt.Errorf("failed to write temp config %s: %v", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temp config %s: %v", tmpName, err)
	}
	// CreateTemp makes the file 0600. Set the mode explicitly BEFORE the rename so the config is
	// never briefly in place with the wrong permissions, and so the runtime user can rewrite it.
	// Chmod is used rather than relying on a perm argument because os.CreateTemp takes none and
	// any perm argument would in any case be masked by the process umask.
	if err := os.Chmod(tmpName, configFileMode); err != nil {
		return fmt.Errorf("failed to chmod temp config %s: %v", tmpName, err)
	}
	if err := os.Rename(tmpName, c.path); err != nil {
		return fmt.Errorf("failed to rename %s to %s: %v", tmpName, c.path, err)
	}
	return nil
}
