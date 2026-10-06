package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// EventTaskTerminal 是任务终态事件；服务端发送它之后关闭流（规格 §15.2）。
const EventTaskTerminal = "task_terminal"

type eventJSON struct {
	TaskSeq   int64           `json:"task_seq"`
	AttemptID string          `json:"attempt_id,omitempty"`
	Source    string          `json:"source"`
	Type      string          `json:"type"`
	WorkerSeq int64           `json:"worker_seq,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	TS        time.Time       `json:"ts"`
}

// parseCursor 解析 Last-Event-ID：缺省为 0（从头开始），否则须为非负十进制整数。
func parseCursor(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, true
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != s {
		return 0, false
	}
	return n, true
}

// sseOut 是一条待发送的 SSE 事件：id、event 与单行 JSON data。
type sseOut struct {
	id    int64
	event string
	data  []byte
}

// ssePage 读取 cursor 之后的一页记录：返回要发送的事件（不可见的记录不产生事件）、新的游标（含不可见记录）、
// 发送后是否关闭流，以及是否读满一页（读满时立即再读，否则等待 PollInterval）。
type ssePage func(ctx context.Context, cursor int64) (out []sseOut, next int64, done, full bool, err error)

// runSSE 发送事件流（规格 §15.2）：写出流式响应头后循环读取 page，注释行心跳；page 报告 done 时发送完该页后关闭。
// 读取失败时关闭流（头已发送，无法再返回错误码），客户端以 Last-Event-ID 续传。
func (h *Handler) runSSE(w http.ResponseWriter, r *http.Request, cursor int64, page ssePage) {
	ctx := r.Context()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal", "响应不支持流式输出")
		return
	}
	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()
	poll := time.NewTimer(0)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-poll.C:
			out, next, done, full, err := page(ctx, cursor)
			if err != nil {
				return
			}
			for _, f := range out {
				if err := writeFrame(w, f); err != nil {
					return
				}
			}
			flusher.Flush()
			if done {
				return
			}
			cursor = next
			if full {
				poll.Reset(0)
			} else {
				poll.Reset(h.cfg.PollInterval)
			}
		}
	}
}

// streamEvents 以 SSE 发送任务事件：id = task_seq，Last-Event-ID 续传，注释行心跳，
// task_terminal 之后关闭（规格 §15.2）。
func (h *Handler) streamEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	taskID := r.PathValue("id")
	cursor, ok := parseCursor(r.Header.Get("Last-Event-ID"))
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_cursor", "Last-Event-ID 须为非负整数")
		return
	}
	if _, err := h.cfg.Store.GetTask(ctx, taskID); err != nil {
		writeStoreError(w, err)
		return
	}
	if cursor > 0 { // 游标须是已存在的事件：task_seq ≥ cursor 的事件存在
		evs, err := h.cfg.Store.ListEvents(ctx, taskID, cursor-1, 1)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		if len(evs) == 0 {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "Last-Event-ID 超出最新事件")
			return
		}
	}
	user := isUserCaller(ctx)
	h.runSSE(w, r, cursor, func(ctx context.Context, cursor int64) ([]sseOut, int64, bool, bool, error) {
		evs, err := h.cfg.Store.ListEvents(ctx, taskID, cursor, eventPageSize)
		if err != nil {
			return nil, cursor, false, false, err
		}
		var out []sseOut
		for _, ev := range evs {
			shown, keep := ev, true
			if user {
				shown, keep = userEvent(ev) // 用户只看到允许列表中的事件与字段；被丢弃的事件游标照常前进
			}
			if keep {
				data, err := json.Marshal(eventJSON(shown))
				if err != nil {
					return out, cursor, false, false, err
				}
				out = append(out, sseOut{id: shown.TaskSeq, event: shown.Type, data: data})
			}
			cursor = ev.TaskSeq
			if ev.Type == EventTaskTerminal {
				return out, cursor, true, false, nil
			}
		}
		return out, cursor, false, len(evs) == eventPageSize, nil
	})
}

// writeFrame 写出一条 SSE 事件。事件类型来自固定集合；仍去掉换行以免破坏帧格式。
func writeFrame(w http.ResponseWriter, f sseOut) error {
	typ := strings.NewReplacer("\r", "", "\n", "").Replace(f.event)
	_, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", f.id, typ, f.data)
	return err
}
