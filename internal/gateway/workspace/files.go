package workspace

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// ---- read_file ----

type readReq struct {
	Path      string `json:"path"`
	StartLine *int   `json:"start_line"`
	MaxLines  *int   `json:"max_lines"`
}

type readResp struct {
	Path      string `json:"path"`
	Content   string `json:"content"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	// TotalLines is known only when the read reached the end of the file (nil otherwise).
	TotalLines *int   `json:"total_lines"`
	Truncated  bool   `json:"truncated"`
	Binary     bool   `json:"binary"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
}

// Read handles POST /v1/workspace/read. The blob is streamed: lines before start_line are skipped, at most
// max_lines lines and MaxReadBytes bytes are kept, and binary detection looks at that window only.
func (m *Manager) Read(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	var q readReq
	defer func() { m.logOp("read", r, start, res, "path", q.Path) }()
	if code, ok := decode(r.Body, &q); !ok {
		return fail(http.StatusBadRequest, code), nil
	}
	first, max := 1, MaxReadLines
	if q.StartLine != nil {
		first = *q.StartLine
	}
	if q.MaxLines != nil {
		max = *q.MaxLines
	}
	if first < 1 || max < 1 || max > MaxReadLines {
		return fail(http.StatusBadRequest, CodeInvalidRequest), nil
	}
	if !ValidPath(q.Path) {
		return fail(http.StatusBadRequest, CodeInvalidPath), nil
	}
	_, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	e, ok := st.Files[q.Path]
	isDir := !ok && hasDir(st, q.Path)
	unlock()
	if isDir {
		return fail(http.StatusBadRequest, CodeIsDirectory), nil
	}
	if !ok {
		return fail(http.StatusNotFound, CodeNotFound), nil
	}
	rc, err := m.cfg.Blobs.Open(e.SHA256)
	if err != nil {
		return call.Result{}, err
	}
	defer rc.Close()
	w, err := readWindow(rc, first, max, MaxReadBytes)
	if err != nil {
		return call.Result{}, err
	}
	out := readResp{Path: q.Path, StartLine: first, Size: e.Size, SHA256: e.SHA256, Truncated: w.truncated,
		TotalLines: w.total}
	if w.binary {
		out.Binary, out.EndLine = true, first-1
		return okJSON(out)
	}
	out.Content, out.EndLine = string(w.data), first-1+w.lines
	return okJSON(out)
}

type window struct {
	data      []byte
	lines     int  // lines (or partial last line) in data
	truncated bool // more content after the window
	binary    bool
	total     *int // total lines, when the end of the file was reached
}

// readWindow streams r: skips lines before first, keeps at most maxLines lines and maxBytes bytes.
func readWindow(r io.Reader, first, maxLines, maxBytes int) (window, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	var w window
	line := 1 // number of the line the reader is positioned at
	partial := false
	for line < first {
		chunk, err := br.ReadSlice('\n')
		switch {
		case err == nil:
			line, partial = line+1, false
		case errors.Is(err, bufio.ErrBufferFull): // same line continues
			partial = true
		case errors.Is(err, io.EOF):
			total := line - 1
			if len(chunk) > 0 || partial {
				total = line
			}
			w.total = &total
			return w, nil
		default:
			return w, err
		}
	}
	var buf bytes.Buffer
	partial = false
	for w.lines < maxLines {
		chunk, err := br.ReadSlice('\n')
		if buf.Len()+len(chunk) > maxBytes {
			buf.Write(chunk[:maxBytes-buf.Len()])
			w.truncated = true
			w.lines++
			break
		}
		buf.Write(chunk)
		switch {
		case err == nil:
			w.lines, partial = w.lines+1, false
			continue
		case errors.Is(err, bufio.ErrBufferFull):
			partial = true
			continue
		case errors.Is(err, io.EOF):
			if len(chunk) > 0 || partial {
				w.lines++
			}
			total := first - 1 + w.lines
			w.total = &total
		default:
			return w, err
		}
		break
	}
	if w.total == nil && !w.truncated {
		if _, err := br.Peek(1); errors.Is(err, io.EOF) {
			total := first - 1 + w.lines
			w.total = &total
		} else {
			w.truncated = true
		}
	}
	data := buf.Bytes()
	if w.truncated { // a cut multi-byte character at the end is not binary content
		for k := 1; k < utf8.UTFMax && k <= len(data); k++ {
			if utf8.RuneStart(data[len(data)-k]) {
				if !utf8.FullRune(data[len(data)-k:]) {
					data = data[:len(data)-k]
				}
				break
			}
		}
	}
	w.binary = !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0
	w.data = data
	return w, nil
}

// ---- write_file (and delete) ----

type writeReq struct {
	Path       string  `json:"path"`
	Content    *string `json:"content"`
	Executable *bool   `json:"executable"`
	Delete     bool    `json:"delete"`
}

type writeResp struct {
	Path      string   `json:"path"`
	Size      int64    `json:"size"`
	SHA256    string   `json:"sha256"`
	Created   bool     `json:"created"`
	Deleted   []string `json:"deleted,omitempty"`
	Workspace summary  `json:"workspace"`
}

// maxDeletePathBytes bounds a delete path (delete accepts any existing manifest key, even one that is not a valid path
// any more, so a bad entry can always be removed).
const maxDeletePathBytes = 4096

// Write handles POST /v1/workspace/write: {"path","content","executable"?} writes a file; {"path","delete":true}
// deletes a file or a whole directory.
func (m *Manager) Write(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	var q writeReq
	defer func() { m.logOp("write", r, start, res, "path", q.Path, "delete", q.Delete) }()
	if code, ok := decode(r.Body, &q); !ok {
		return fail(http.StatusBadRequest, code), nil
	}
	if q.Delete {
		if q.Content != nil || q.Executable != nil || q.Path == "" || len(q.Path) > maxDeletePathBytes {
			return fail(http.StatusBadRequest, CodeInvalidRequest), nil
		}
		return m.delete(ctx, r, q.Path)
	}
	if q.Content == nil {
		return fail(http.StatusBadRequest, CodeInvalidRequest), nil
	}
	if !ValidPath(q.Path) {
		return fail(http.StatusBadRequest, CodeInvalidPath), nil
	}
	if len(*q.Content) > MaxWriteBytes {
		return fail(http.StatusRequestEntityTooLarge, CodeContentTooLarge), nil
	}
	w, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	defer unlock()
	size := int64(len(*q.Content))
	if st.WriteOps >= m.cfg.MaxWriteOps || st.WrittenBytes+size > m.cfg.MaxWrittenBytes {
		return fail(http.StatusTooManyRequests, CodeWriteQuota), nil
	}
	old, exists := st.Files[q.Path]
	if code := m.checkPlace(st, q.Path, exists); code != "" {
		return fail(http.StatusConflict, code), nil
	}
	if st.bytes()-old.Size+size > m.cfg.MaxBytes {
		return fail(http.StatusConflict, CodeWorkspaceFull), nil
	}
	ref, err := m.cfg.Blobs.Put(ctx, strings.NewReader(*q.Content))
	if err != nil {
		return call.Result{}, err
	}
	exec := old.Exec
	if q.Executable != nil {
		exec = *q.Executable
	}
	next := st.clone()
	next.Files[q.Path] = entry{SHA256: ref.SHA256, Size: ref.Size, Exec: exec}
	next.WriteOps++
	next.WrittenBytes += size
	if err := m.commit(w, next); err != nil {
		m.log.Error("gateway: workspace state write failed", "task_id", r.TaskID, "err", err)
		return fail(http.StatusServiceUnavailable, CodeStoreUnavailable), nil
	}
	return okJSON(writeResp{Path: q.Path, Size: ref.Size, SHA256: ref.SHA256, Created: !exists, Workspace: summarize(next)})
}

// checkPlace: a new file may not be a directory of other files, nor lie under a file, nor exceed the file count.
func (m *Manager) checkPlace(st *state, p string, exists bool) string {
	if exists {
		return ""
	}
	if hasDir(st, p) {
		return CodePathConflict
	}
	for d := parent(p); d != ""; d = parent(d) {
		if _, isFile := st.Files[d]; isFile {
			return CodePathConflict
		}
	}
	if len(st.Files) >= m.cfg.MaxFiles {
		return CodeWorkspaceFull
	}
	return ""
}

// delete removes an exact manifest key, or every file under a directory.
func (m *Manager) delete(ctx context.Context, r Request, p string) (call.Result, error) {
	w, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	defer unlock()
	next := st.clone()
	var gone []string
	if _, ok := next.Files[p]; ok {
		gone = append(gone, p)
	} else if validDir(p) {
		for f := range next.Files {
			if under(f, p) {
				gone = append(gone, f)
			}
		}
	}
	if len(gone) == 0 {
		return fail(http.StatusNotFound, CodeNotFound), nil
	}
	sort.Strings(gone)
	for _, f := range gone {
		delete(next.Files, f)
	}
	if err := m.commit(w, next); err != nil {
		m.log.Error("gateway: workspace state write failed", "task_id", r.TaskID, "err", err)
		return fail(http.StatusServiceUnavailable, CodeStoreUnavailable), nil
	}
	return okJSON(writeResp{Path: p, Deleted: gone, Workspace: summarize(next)})
}

// ---- list_dir ----

type listReq struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

type listEntry struct {
	Name       string `json:"name"`
	Type       string `json:"type"` // file | dir
	Size       int64  `json:"size,omitempty"`
	Executable bool   `json:"executable,omitempty"`
}

type listResp struct {
	Path      string      `json:"path"`
	Entries   []listEntry `json:"entries"`
	Workspace summary     `json:"workspace"`
}

// List handles POST /v1/workspace/list.
func (m *Manager) List(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	var q listReq
	defer func() { m.logOp("list", r, start, res, "path", q.Path) }()
	if len(bytes.TrimSpace(r.Body)) > 0 {
		if code, ok := decode(r.Body, &q); !ok {
			return fail(http.StatusBadRequest, code), nil
		}
	}
	if q.Path == "." {
		q.Path = ""
	}
	if !validDir(q.Path) {
		return fail(http.StatusBadRequest, CodeInvalidPath), nil
	}
	_, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	defer unlock()
	if _, isFile := st.Files[q.Path]; isFile {
		return fail(http.StatusBadRequest, CodeNotDirectory), nil
	}
	if q.Path != "" && !hasDir(st, q.Path) {
		return fail(http.StatusNotFound, CodeNotFound), nil
	}
	return okJSON(listResp{Path: q.Path, Entries: entries(st, q.Path, q.Recursive), Workspace: summarize(st)})
}

func entries(st *state, dir string, recursive bool) []listEntry {
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	seen := map[string]bool{}
	out := []listEntry{}
	for p, e := range st.Files {
		if !under(p, dir) {
			continue
		}
		rel := strings.TrimPrefix(p, prefix)
		if i := strings.IndexByte(rel, '/'); i >= 0 && !recursive {
			if d := rel[:i]; !seen[d] {
				seen[d] = true
				out = append(out, listEntry{Name: d, Type: "dir"})
			}
			continue
		}
		out = append(out, listEntry{Name: rel, Type: "file", Size: e.Size, Executable: e.Exec})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}
