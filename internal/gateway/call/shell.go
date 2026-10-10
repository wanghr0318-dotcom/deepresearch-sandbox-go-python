package call

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// Workspace shell commands (design 2026-10-10-shell-file-mcp §3.1): ExecShell runs one bash command in a fresh exec
// environment through the same pipeline as POST /v1/exec — journal (calls.endpoint = /v1/exec), exec slots, exec
// quota, stop/collect/cleanup, replay by call id. What differs is the variant:
//
//   - inputs are the workspace files held by the Gateway (trusted: they come from the Gateway's own manifest, not
//     from worker-supplied sha256s), staged read-only under /in/ws/ (0444, 0555 for executables);
//   - the command is staged as /in/.agentbox/cmd.sh and run by a fixed wrapper that copies /in/ws/. into /out
//     (the writable tmpfs, preserving mode bits) and runs the command there;
//   - the fingerprint has endpoint /v1/workspace/exec and adapter version exec-shell/1, so a shell call can never
//     replay as, or collide with, a /v1/exec call;
//   - outputs carry the executable bit (fstat on the opened file).

// ShellEndpoint is the Gateway endpoint of workspace commands (fingerprint and divergence events only; the call is
// journaled and reserved as an exec call).
const ShellEndpoint = "/v1/workspace/exec"

// ShellAdapterVersion enters the shell fingerprint.
const ShellAdapterVersion = "exec-shell/1"

// MaxShellCommandBytes bounds the command text.
const MaxShellCommandBytes = 64 << 10

// shellCmdRel is the staged command file under /in.
const shellCmdRel = ".agentbox/cmd.sh"

// ShellWorkspaceDir is where the workspace is staged inside /in.
const ShellWorkspaceDir = "ws"

// shellWrapper is the fixed program run by bash -c (not caller-controlled). Staging failures exit 125 with a
// message on stderr so they are distinguishable from the command's own exit codes.
const shellWrapper = `if [ -d /in/ws ]; then
  cp -R --preserve=mode /in/ws/. /out/ && chmod -R u+w /out || { echo "agentbox: workspace staging failed" >&2; exit 125; }
fi
cd /out || exit 125
printf '\000agentbox:staged\n'
exec /bin/bash --noprofile --norc /in/.agentbox/cmd.sh`

// StagedMarker is what the wrapper writes to stdout after the workspace was copied completely and before the command
// starts: positionally unforgeable (the command can only write after it), so its presence at the very start of stdout
// is the wrapper's positive "staged OK" signal. ExecShell strips it and reports workspace_staged.
var StagedMarker = []byte("\x00agentbox:staged\n")

// ShellArgv is the fixed argv of a workspace command.
func ShellArgv() []string {
	return []string{"/bin/bash", "--noprofile", "--norc", "-c", shellWrapper}
}

// ShellEnviron is ExecEnviron plus WORKSPACE=/out.
func ShellEnviron() []string { return append(ExecEnviron(), "WORKSPACE=/out") }

// ShellFile is one workspace file to stage.
type ShellFile struct {
	Path       string // relative workspace path
	SHA256     string
	Size       int64
	Executable bool
}

// ShellInvoke is one workspace command.
type ShellInvoke struct {
	TaskID, AttemptID, CallID, SubrunID string
	Command                             string
	WallMs                              int64 // 0 = default; capped by ExecConfig.Max
	Files                               []ShellFile
	Retry                               bool
}

// shellReq is the validated shell variant of execParsed.
type shellReq struct {
	command string
	files   []ShellFile // sorted by path
}

// ExecShell runs a workspace command (see the comment at the top of this file). Rejections and failures are returned
// as Result.Status/Code with a nil error, as for Exec; a non-nil error is an internal failure or ctx ending (the
// command continues in the background and settles, so the same call id replays it).
func (c *Coordinator) ExecShell(ctx context.Context, in ShellInvoke) (res Result, err error) {
	ctx, span := obs.Start(ctx, "gateway.call", callAttrs(kindExecShell, in.TaskID, in.AttemptID, in.CallID, in.SubrunID)...)
	defer func() { endCall(span, kindExecShell, in.TaskID, res, err) }()
	return c.shellCall(ctx, in)
}

// kindExecShell is the observability kind of workspace commands (gateway.kind, metric kind label).
const kindExecShell = "exec_shell"

func (c *Coordinator) shellCall(ctx context.Context, in ShellInvoke) (Result, error) {
	if in.TaskID == "" || in.AttemptID == "" || in.CallID == "" {
		return Result{}, fmt.Errorf("%w: ExecShell 缺少 task_id、attempt_id 或 call_id", persistence.ErrInvalid)
	}
	if c.root.Err() != nil {
		return Result{}, ErrClosed
	}
	if c.exec == nil {
		return reject(CodeEndpointNotConfigured), nil
	}
	req, err := c.parseShell(in)
	if err != nil {
		return reject(upstream.CodeInvalidRequest), nil
	}
	fp, err := execFingerprint(req, c.exec.ImageDigest)
	if err != nil {
		return reject(upstream.CodeInvalidRequest), nil
	}
	return c.startExec(ctx, ExecInvoke{TaskID: in.TaskID, AttemptID: in.AttemptID, CallID: in.CallID, SubrunID: in.SubrunID,
		Retry: in.Retry}, req, fp)
}

func (c *Coordinator) parseShell(in ShellInvoke) (execParsed, error) {
	if in.Command == "" || len(in.Command) > MaxShellCommandBytes || !utf8.ValidString(in.Command) ||
		strings.ContainsRune(in.Command, 0) || in.WallMs < 0 {
		return execParsed{}, fmt.Errorf("command")
	}
	files := append([]ShellFile(nil), in.Files...)
	sort.Slice(files, func(a, b int) bool { return files[a].Path < files[b].Path })
	seen := map[string]bool{}
	for _, f := range files {
		if !validInputPath(ShellWorkspaceDir+"/"+f.Path) || !validInputPath(f.Path) || !isSHA256Hex(f.SHA256) ||
			f.Size < 0 || seen[f.Path] {
			return execParsed{}, fmt.Errorf("file %q", f.Path)
		}
		seen[f.Path] = true
	}
	for _, f := range files {
		for d := path.Dir(f.Path); d != "."; d = path.Dir(d) {
			if seen[d] {
				return execParsed{}, fmt.Errorf("file %q under file %q", f.Path, d)
			}
		}
	}
	limits := c.exec.Default
	if in.WallMs > 0 {
		limits.WallMs = min(in.WallMs, c.exec.Max.WallMs)
	}
	return execParsed{inputs: []execInput{}, limits: limits, shell: &shellReq{command: in.Command, files: files}}, nil
}

// shellFingerprint is sha256(JCS({endpoint, adapter_version, resolved: {command_sha256, files (sorted by path),
// image_digest, limits}})).
func shellFingerprint(p execParsed, imageDigest string) (string, error) {
	type file struct {
		Path       string `json:"path"`
		SHA256     string `json:"sha256"`
		Executable bool   `json:"executable"`
	}
	type limits struct {
		WallMs      int64 `json:"wall_ms"`
		MemoryBytes int64 `json:"memory_bytes"`
	}
	type resolved struct {
		CommandSHA256 string `json:"command_sha256"`
		Files         []file `json:"files"`
		ImageDigest   string `json:"image_digest"`
		Limits        limits `json:"limits"`
	}
	files := make([]file, 0, len(p.shell.files))
	for _, f := range p.shell.files {
		files = append(files, file{f.Path, f.SHA256, f.Executable})
	}
	sum := sha256.Sum256([]byte(p.shell.command))
	canon, err := jcs.Canonical(struct {
		Endpoint       string   `json:"endpoint"`
		AdapterVersion string   `json:"adapter_version"`
		Resolved       resolved `json:"resolved"`
	}{ShellEndpoint, ShellAdapterVersion, resolved{hex.EncodeToString(sum[:]), files, imageDigest,
		limits{p.limits.WallMs, p.limits.MemoryBytes}}})
	if err != nil {
		return "", err
	}
	fp := sha256.Sum256(canon)
	return hex.EncodeToString(fp[:]), nil
}

// stageShell writes the command and the workspace files (under ws/) into /in; the total is bounded like exec inputs.
func (c *Coordinator) stageShell(inDir string, s *shellReq) (string, error) {
	if _, err := writeStaged(inDir, shellCmdRel, strings.NewReader(s.command), -1, 0o444); err != nil {
		return CodeExecEnvUnavailable, err
	}
	if len(s.files) == 0 {
		return "", nil
	}
	var total int64
	for _, f := range s.files {
		rc, err := c.blobs.Open(f.SHA256)
		if err != nil {
			return CodeExecEnvUnavailable, fmt.Errorf("打开工作区文件 %s: %w", f.Path, err)
		}
		mode := os.FileMode(0o444)
		if f.Executable {
			mode = 0o555
		}
		n, err := writeStaged(inDir, ShellWorkspaceDir+"/"+f.Path, rc, c.execInputMax-total, mode)
		if cerr := rc.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			if errors.Is(err, errInputsTooLarge) {
				return CodeInputsTooLarge, err
			}
			return CodeExecEnvUnavailable, err
		}
		total += n
	}
	return "", nil
}

// executable reports whether an opened output file has any execute bit (fstat on the FD that is being collected,
// so it describes exactly the file whose content is saved).
func executable(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode().Perm()&0o111 != 0
}
