package api

import (
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

	user := isUserCaller(ctx)
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
			evs, err := h.cfg.Store.ListEvents(ctx, taskID, cursor, eventPageSize)
			if err != nil {
				// 头已发送，无法再返回错误码；关闭流，客户端以 Last-Event-ID 续传。
				return
			}
			for _, ev := range evs {
				out, keep := ev, true
				if user {
					out, keep = userEvent(ev) // 用户只看到允许列表中的事件与字段；被丢弃的事件游标照常前进
				}
				if keep {
					if err := writeEvent(w, out); err != nil {
						return
					}
				}
				cursor = ev.TaskSeq
				if ev.Type == EventTaskTerminal {
					flusher.Flush()
					return
				}
			}
			flusher.Flush()
			if len(evs) == eventPageSize {
				poll.Reset(0)
			} else {
				poll.Reset(h.cfg.PollInterval)
			}
		}
	}
}

func writeEvent(w http.ResponseWriter, ev Event) error {
	data, err := json.Marshal(eventJSON(ev))
	if err != nil {
		return err
	}
	// 事件类型来自宿主写入的固定集合；去掉换行以免破坏帧格式。
	typ := strings.NewReplacer("\r", "", "\n", "").Replace(ev.Type)
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.TaskSeq, typ, data)
	return err
}
