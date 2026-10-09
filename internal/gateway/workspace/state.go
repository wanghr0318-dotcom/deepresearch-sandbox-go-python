package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
)

// Workspace states (design §3.2).
const (
	StateActive  = "active"
	StateExpired = "expired" // idle timeout: manifest dropped, tombstone kept until the task ends
	StateLost    = "lost"    // state file unreadable/inconsistent or a file blob missing after a restart
)

// maxApplied is how many applied exec call ids a workspace remembers for idempotent replay.
const maxApplied = 32

// entry is one file of the manifest.
type entry struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Exec   bool   `json:"exec,omitempty"`
}

// applied records an exec call that advanced the head: its combined response blob and the journaled exec result blob.
type applied struct {
	CallID         string `json:"call_id"`
	ResponseSHA256 string `json:"response_sha256"`
	ResultSHA256   string `json:"result_sha256"`
}

// state is the durable state of one workspace (<dir>/<sha256(task_id)[:32]>.json).
type state struct {
	TaskID    string           `json:"task_id"`
	State     string           `json:"state"`
	Reason    string           `json:"reason,omitempty"`
	Version   string           `json:"version"`
	Files     map[string]entry `json:"files"`
	CreatedAt time.Time        `json:"created_at"`
	UsedAt    time.Time        `json:"used_at"`
	Ops       int64            `json:"ops"`
	Applied   []applied        `json:"applied,omitempty"`
}

// version is sha256(JCS(files)) — the content identity of the manifest.
func version(files map[string]entry) string {
	if files == nil {
		files = map[string]entry{}
	}
	b, err := jcs.Canonical(files)
	if err != nil { // a map of plain structs always canonicalizes
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s *state) bytes() int64 {
	var n int64
	for _, e := range s.Files {
		n += e.Size
	}
	return n
}

func (s *state) clone() *state {
	c := *s
	c.Files = make(map[string]entry, len(s.Files))
	for k, v := range s.Files {
		c.Files[k] = v
	}
	c.Applied = append([]applied(nil), s.Applied...)
	return &c
}

func (s *state) findApplied(callID string) (applied, bool) {
	for _, a := range s.Applied {
		if a.CallID == callID {
			return a, true
		}
	}
	return applied{}, false
}

func (s *state) addApplied(a applied) {
	s.Applied = append(s.Applied, a)
	if n := len(s.Applied); n > maxApplied {
		s.Applied = append([]applied(nil), s.Applied[n-maxApplied:]...)
	}
}

// fileName is the state file of a task: a hash, so arbitrary task ids never become path components.
func fileName(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return hex.EncodeToString(sum[:16]) + ".json"
}

// errCorrupt marks a state file that exists but cannot be trusted.
var errCorrupt = errors.New("workspace: corrupt state")

// readState loads a state file. Missing → (nil, nil). Unreadable or inconsistent → errCorrupt (wrapped).
func readState(path, taskID string) (*state, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCorrupt, err)
	}
	var s state
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%w: %v", errCorrupt, err)
	}
	if taskID != "" && s.TaskID != taskID {
		return nil, fmt.Errorf("%w: task_id %q", errCorrupt, s.TaskID)
	}
	switch s.State {
	case StateActive:
		if s.Files == nil {
			s.Files = map[string]entry{}
		}
		for p, e := range s.Files {
			if !ValidPath(p) || len(e.SHA256) != 64 || e.Size < 0 {
				return nil, fmt.Errorf("%w: file %q", errCorrupt, p)
			}
		}
		if version(s.Files) != s.Version {
			return nil, fmt.Errorf("%w: version mismatch", errCorrupt)
		}
	case StateExpired, StateLost:
		s.Files = map[string]entry{}
	default:
		return nil, fmt.Errorf("%w: state %q", errCorrupt, s.State)
	}
	return &s, nil
}

// writeState writes the state atomically: temp file, fsync, rename, fsync of the directory.
func writeState(dir string, s *state) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	name := filepath.Join(dir, fileName(s.TaskID))
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(b)
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), name)
	}
	if err != nil {
		_ = os.Remove(tmp.Name()) // best effort; the error that matters is err
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
