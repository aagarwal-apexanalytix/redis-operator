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

func (c *Config) Commit() error {
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dir, err)
	}
	if err := os.WriteFile(c.path, []byte(c.content), configFileMode); err != nil {
		return err
	}
	// os.WriteFile's perm argument is masked by the process umask (so 0666 lands as 0644 under a
	// typical 022), and is ignored ENTIRELY when the file already exists — which it does on every
	// restart after the first. Set the mode explicitly so neither case can silently reintroduce a
	// read-only config.
	return os.Chmod(c.path, configFileMode)
}
