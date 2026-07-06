// Package installer wires the user's coding agents to the local host
// daemon and (optionally) registers the daemon as a per-user system
// service.
//
// Reversible: every file we touch can be cleanly reverted via Uninstall.
// We never overwrite a hook entry we didn't write — installs merge in,
// uninstalls remove only our marked entries.
//
// The marker is the literal string "ambient-link-host" appearing in the URL
// (or command, for Codex) of each hook entry — easy to grep, easy to
// reverse.
package installer

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// HookEntryMarker is the substring we put into every hook URL/command so
// Uninstall can identify and remove only what Install created.
const HookEntryMarker = "ambient-link-host"

// Options drives Install / Uninstall behavior.
type Options struct {
	// HostURL is the base URL the agents POST hook events to. Typically
	// http://127.0.0.1:5181. Required for Install.
	HostURL string
	// BearerToken is added as `Authorization: Bearer …` on hook POSTs.
	// Generated automatically by Install if empty.
	BearerToken string
	// AgentSettings overrides path discovery for testing.
	ClaudeSettingsPath string
	CodexHooksPath     string
	// ServiceName for launchd/systemd. Default "com.maceip.fm.host".
	ServiceName string
	// BinaryPath the service unit invokes; default current executable.
	BinaryPath string
	// SkipService skips writing the launchd plist / systemd unit.
	SkipService bool
	Logger      *slog.Logger
}

// InstallResult tells the caller what got changed (so the CLI can print
// a useful summary).
type InstallResult struct {
	ClaudeSettingsModified bool
	CodexHooksModified     bool
	ServiceUnitPath        string
	ServiceUnitCreated     bool
	BearerToken            string
}

// Install wires up the user's agents. Existing settings.json content is
// preserved (we deep-merge). Idempotent: re-running with the same options
// is a no-op.
func Install(opts Options) (*InstallResult, error) {
	if opts.HostURL == "" {
		return nil, errors.New("HostURL required")
	}
	if _, err := url.Parse(opts.HostURL); err != nil {
		return nil, fmt.Errorf("invalid HostURL: %w", err)
	}
	if opts.ServiceName == "" {
		opts.ServiceName = "com.maceip.fm.host"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.BearerToken == "" {
		tok, err := randomToken(24)
		if err != nil {
			return nil, fmt.Errorf("token generation: %w", err)
		}
		opts.BearerToken = tok
	}
	if opts.BinaryPath == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("locate self: %w", err)
		}
		opts.BinaryPath = exe
	}

	res := &InstallResult{BearerToken: opts.BearerToken}

	// Claude Code hooks
	claudePath := opts.ClaudeSettingsPath
	if claudePath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		claudePath = filepath.Join(home, ".claude", "settings.json")
	}
	changed, err := installClaudeHooks(claudePath, opts.HostURL, opts.BearerToken)
	if err != nil {
		return nil, fmt.Errorf("claude hooks: %w", err)
	}
	res.ClaudeSettingsModified = changed

	// Codex hooks
	codexPath := opts.CodexHooksPath
	if codexPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		codexPath = filepath.Join(home, ".codex", "hooks.json")
	}
	changed, err = installCodexHooks(codexPath, opts.HostURL, opts.BearerToken)
	if err != nil {
		return nil, fmt.Errorf("codex hooks: %w", err)
	}
	res.CodexHooksModified = changed

	// Service unit
	if !opts.SkipService {
		path, created, err := installService(opts.ServiceName, opts.BinaryPath, opts.HostURL, opts.BearerToken)
		if err != nil {
			return nil, fmt.Errorf("service unit: %w", err)
		}
		res.ServiceUnitPath = path
		res.ServiceUnitCreated = created
	}

	return res, nil
}

// Uninstall reverses Install. Hook entries we created are stripped; service
// unit files we created are removed. Other settings in the agent config
// files are left intact.
func Uninstall(opts Options) error {
	if opts.ServiceName == "" {
		opts.ServiceName = "com.maceip.fm.host"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	claudePath := opts.ClaudeSettingsPath
	if claudePath == "" {
		claudePath = filepath.Join(home, ".claude", "settings.json")
	}
	codexPath := opts.CodexHooksPath
	if codexPath == "" {
		codexPath = filepath.Join(home, ".codex", "hooks.json")
	}
	if err := uninstallClaudeHooks(claudePath); err != nil {
		opts.Logger.Warn("uninstall: claude hooks", "err", err)
	}
	if err := uninstallCodexHooks(codexPath); err != nil {
		opts.Logger.Warn("uninstall: codex hooks", "err", err)
	}
	if !opts.SkipService {
		if err := uninstallService(opts.ServiceName); err != nil {
			opts.Logger.Warn("uninstall: service", "err", err)
		}
	}
	return nil
}

// ── Claude Code: ~/.claude/settings.json ────────────────────────────────

// Claude Code's hooks schema (per code.claude.com/docs/en/hooks) is a top-level
// "hooks" object mapping event names to arrays of hook entries. Each entry has
// "matcher" (optional) and "handler" (type + url/command). We install entries
// for the events that matter for session lifecycle.
type claudeSettings = map[string]any

// Claude Code's real hooks schema (code.claude.com/docs/en/hooks): each event
// maps to matcher entries whose nested "hooks" array holds COMMAND hooks. The
// command receives the event JSON on stdin and its stdout (exit 0) is parsed
// as the hook response — which is exactly the JSON delivery/hooks.go returns
// (Stop → {"decision":"block","reason":<queued text>} etc.). There is no
// "http" handler type; the previous installer wrote one and Claude Code
// silently ignored every entry, so the hooks delivery path never fired.
type claudeHookEntry struct {
	Matcher string          `json:"matcher,omitempty"`
	Hooks   []claudeHookCmd `json:"hooks"`
}
type claudeHookCmd struct {
	Type    string `json:"type"` // "command"
	Command string `json:"command"`
	Timeout int    `json:"timeout,omitempty"` // seconds
}

// claudeHookCommand builds the curl bridge: stdin (event JSON) → relay hooks
// endpoint → response JSON on stdout for Claude Code to act on. -f maps relay
// errors to a non-zero exit (non-blocking for the agent); -m keeps a dead
// relay from stalling the session.
func claudeHookCommand(hostURL, token string) string {
	url := strings.TrimRight(hostURL, "/") + "/ambient-link/hooks/claude?marker=" + HookEntryMarker
	cmd := "curl -fsS -m 5 -X POST -H 'Content-Type: application/json'"
	if token != "" {
		cmd += " -H 'Authorization: Bearer " + token + "'"
	}
	return cmd + " --data-binary @- '" + url + "'"
}

func installClaudeHooks(path, hostURL, token string) (bool, error) {
	settings, err := readJSONOrEmpty(path)
	if err != nil {
		return false, err
	}
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		hooks = map[string]any{}
	}

	entriesFor := func(event string) []any {
		raw, _ := hooks[event].([]any)
		return raw
	}

	want := []struct {
		event   string
		matcher string
	}{
		{"SessionStart", ""},
		{"SessionEnd", ""},
		{"UserPromptSubmit", ""},
		{"PreToolUse", ""},
		{"PostToolUse", ""},
		{"Notification", "permission_prompt"},
		{"PermissionRequest", ""},
		{"Stop", ""},
		{"SubagentStop", ""},
	}
	changed := false
	for _, w := range want {
		existing := entriesFor(w.event)
		entry := claudeHookEntry{
			Matcher: w.matcher,
			Hooks: []claudeHookCmd{{
				Type:    "command",
				Command: claudeHookCommand(hostURL, token),
				Timeout: 10,
			}},
		}
		var asMap map[string]any
		mustRoundTrip(entry, &asMap)
		if containsEntry(existing, asMap) {
			continue
		}
		// Replace any stale marker entry (e.g. the legacy "http" handler shape
		// Claude Code ignored) instead of stacking a second one beside it.
		hooks[w.event] = append(filterOutMarker(existing), asMap)
		changed = true
	}
	if !changed {
		return false, nil
	}
	settings["hooks"] = hooks
	return true, writeJSON(path, settings, 0o644)
}

func uninstallClaudeHooks(path string) error {
	settings, err := readJSON(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		return nil
	}
	changed := false
	for event, raw := range hooks {
		arr, ok := raw.([]any)
		if !ok {
			continue
		}
		filtered := filterOutMarker(arr)
		if len(filtered) != len(arr) {
			changed = true
			if len(filtered) == 0 {
				delete(hooks, event)
			} else {
				hooks[event] = filtered
			}
		}
	}
	if !changed {
		return nil
	}
	if len(hooks) == 0 {
		delete(settings, "hooks")
	}
	return writeJSON(path, settings, 0o644)
}

// containsEntry reports whether arr already holds an entry deep-equal to
// want — the idempotency check for re-running install with the same options.
func containsEntry(arr []any, want map[string]any) bool {
	wantJSON, err := json.Marshal(want)
	if err != nil {
		return false
	}
	for _, x := range arr {
		m, ok := x.(map[string]any)
		if !ok || !entryCarriesMarker(m) {
			continue
		}
		got, err := json.Marshal(m)
		if err == nil && string(got) == string(wantJSON) {
			return true
		}
	}
	return false
}

// entryCarriesMarker reports whether one settings entry references our
// endpoint — in a legacy handler {url|command} (both agents' old shapes) or
// in Claude Code's nested hooks[].command array.
func entryCarriesMarker(m map[string]any) bool {
	h, _ := m["handler"].(map[string]any)
	if u, _ := h["url"].(string); strings.Contains(u, HookEntryMarker) {
		return true
	}
	if cmd, _ := h["command"].(string); strings.Contains(cmd, HookEntryMarker) {
		return true
	}
	nested, _ := m["hooks"].([]any)
	for _, n := range nested {
		nm, _ := n.(map[string]any)
		if cmd, _ := nm["command"].(string); strings.Contains(cmd, HookEntryMarker) {
			return true
		}
	}
	return false
}

// hasAmbientLinkHookEntry checks whether any entry in arr carries our marker.
func hasAmbientLinkHookEntry(arr []any) bool {
	for _, x := range arr {
		if m, _ := x.(map[string]any); entryCarriesMarker(m) {
			return true
		}
	}
	return false
}

func filterOutMarker(arr []any) []any {
	out := make([]any, 0, len(arr))
	for _, x := range arr {
		if m, _ := x.(map[string]any); entryCarriesMarker(m) {
			continue
		}
		out = append(out, x)
	}
	return out
}

// ── Codex: ~/.codex/hooks.json (command handlers only — http is silently
// skipped by current codex builds, so we curl) ──────────────────────────

func installCodexHooks(path, hostURL, token string) (bool, error) {
	settings, err := readJSONOrEmpty(path)
	if err != nil {
		return false, err
	}
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		hooks = map[string]any{}
	}

	wantEvents := []string{
		"SessionStart", "SessionEnd",
		"UserPromptSubmit",
		"PreToolUse", "PostToolUse",
		"PermissionRequest",
		"Stop", "SubagentStop",
	}
	url := strings.TrimRight(hostURL, "/") + "/ambient-link/hooks/codex?marker=" + HookEntryMarker
	curlCmd := fmt.Sprintf(
		`curl -sS -X POST -H 'Content-Type: application/json' -H 'Authorization: Bearer %s' --data-binary @- %s`,
		token, url,
	)
	changed := false
	for _, ev := range wantEvents {
		existing, _ := hooks[ev].([]any)
		entry := map[string]any{
			"handler": map[string]any{
				"type":    "command",
				"command": curlCmd,
			},
		}
		if containsEntry(existing, entry) {
			continue
		}
		// Replace stale marker entries (old URL/token) rather than skipping,
		// so a re-run with new options actually takes effect.
		hooks[ev] = append(filterOutMarker(existing), entry)
		changed = true
	}
	if !changed {
		return false, nil
	}
	settings["hooks"] = hooks
	return true, writeJSON(path, settings, 0o644)
}

func uninstallCodexHooks(path string) error {
	// Schema-compatible with Claude removal; reuse.
	return uninstallClaudeHooks(path)
}

// ── service units (launchd / systemd --user) ────────────────────────────

func installService(name, binaryPath, hostURL, token string) (string, bool, error) {
	switch runtime.GOOS {
	case "darwin":
		return installLaunchAgent(name, binaryPath, hostURL, token)
	case "linux":
		return installSystemdUserUnit(name, binaryPath, hostURL, token)
	default:
		return "", false, fmt.Errorf("service install not supported on %s", runtime.GOOS)
	}
}

func uninstallService(name string) error {
	switch runtime.GOOS {
	case "darwin":
		return uninstallLaunchAgent(name)
	case "linux":
		return uninstallSystemdUserUnit(name)
	default:
		return nil
	}
}

func installLaunchAgent(name, binaryPath, hostURL, token string) (string, bool, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", false, err
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", false, err
	}
	plistPath := filepath.Join(dir, name+".plist")
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>serve</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>AMBIENT_LINK_LISTEN</key><string>%s</string>
    <key>AMBIENT_LINK_TOKEN</key><string>%s</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>%s/Library/Logs/%s.log</string>
  <key>StandardOutPath</key><string>%s/Library/Logs/%s.log</string>
</dict>
</plist>
`, name, binaryPath, urlListen(hostURL), token, home, name, home, name)
	if existing, err := os.ReadFile(plistPath); err == nil && string(existing) == plist {
		return plistPath, false, nil
	}
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil {
		return plistPath, false, err
	}
	return plistPath, true, nil
}

func uninstallLaunchAgent(name string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	plistPath := filepath.Join(home, "Library", "LaunchAgents", name+".plist")
	if _, err := os.Stat(plistPath); err != nil {
		return nil
	}
	_ = exec.Command("launchctl", "unload", plistPath).Run()
	return os.Remove(plistPath)
}

func installSystemdUserUnit(name, binaryPath, hostURL, token string) (string, bool, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", false, err
	}
	dir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", false, err
	}
	unitPath := filepath.Join(dir, strings.TrimSuffix(name, ".service")+".service")
	unit := fmt.Sprintf(`[Unit]
Description=ambient-link host daemon
After=network.target

[Service]
ExecStart=%s serve
Restart=on-failure
RestartSec=5
Environment=HOME=%s
Environment=AMBIENT_LINK_LISTEN=%s
Environment=AMBIENT_LINK_TOKEN=%s

[Install]
WantedBy=default.target
`, binaryPath, home, urlListen(hostURL), token)
	if existing, err := os.ReadFile(unitPath); err == nil && string(existing) == unit {
		return unitPath, false, nil
	}
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		return unitPath, false, err
	}
	return unitPath, true, nil
}

func uninstallSystemdUserUnit(name string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	unitPath := filepath.Join(home, ".config", "systemd", "user", strings.TrimSuffix(name, ".service")+".service")
	if _, err := os.Stat(unitPath); err != nil {
		return nil
	}
	_ = exec.Command("systemctl", "--user", "disable", "--now", filepath.Base(unitPath)).Run()
	return os.Remove(unitPath)
}

// ── helpers ────────────────────────────────────────────────────────────

func urlListen(hostURL string) string {
	u, err := url.Parse(hostURL)
	if err != nil || u.Host == "" {
		return "127.0.0.1:5181"
	}
	return u.Host
}

func readJSON(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

func readJSONOrEmpty(path string) (map[string]any, error) {
	m, err := readJSON(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]any{}, nil
	}
	return m, err
}

func writeJSON(path string, v any, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp." + fmt.Sprintf("%d", time.Now().UnixNano())
	if err := os.WriteFile(tmp, append(b, '\n'), mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func mustRoundTrip(in any, out any) {
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
}

func randomToken(nBytes int) (string, error) {
	buf := make([]byte, nBytes)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
