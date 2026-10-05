<template>
  <div class="page">
    <header class="page-header">
      <a class="link back" :href="tasksHref()">← 任务列表</a>
    </header>

    <section class="panel summary">
      <div class="summary-main">
        <div class="title-row">
          <h1 class="page-title mono">{{ id }}</h1>
          <StatusBadge v-if="task" :status="task.status" :title="task.status_reason" />
        </div>
        <dl v-if="task" class="facts">
          <div><dt>desired</dt><dd class="mono">{{ task.desired }}</dd></div>
          <div v-if="task.status_reason"><dt>原因</dt><dd class="mono">{{ task.status_reason }}</dd></div>
          <div><dt>attempts</dt><dd>{{ task.attempts_total }}</dd></div>
          <div>
            <dt>control</dt>
            <dd class="mono">applied {{ task.applied_control_version }} / {{ task.control_version }}</dd>
          </div>
          <div v-if="task.current_attempt_id">
            <dt>当前 attempt</dt><dd class="mono" :title="task.current_attempt_id">{{ shortId(task.current_attempt_id) }}</dd>
          </div>
          <div>
            <dt>事件流</dt>
            <dd><span class="stream" :class="streamState" data-testid="stream-state">{{ streamLabel }}</span></dd>
          </div>
        </dl>
      </div>
      <TaskControls :task="task" @changed="refreshTask" />
    </section>

    <ErrorBanner :error="taskError" />
    <ErrorBanner :error="streamError" />

    <nav class="tabs" role="tablist">
      <button
        v-for="t in TABS"
        :key="t.id"
        class="tab"
        :class="{ active: tab === t.id }"
        type="button"
        role="tab"
        :data-tab="t.id"
        :aria-selected="tab === t.id"
        @click="selectTab(t.id)"
      >
        {{ t.label }}<span v-if="t.id === 'timeline'" class="muted"> ({{ events.length }})</span>
      </button>
    </nav>

    <section v-if="tab === 'timeline'" class="panel">
      <EventTimeline :events="events" :empty-text="streamState === 'connecting' ? '连接事件流…' : '暂无事件'" />
    </section>

    <section v-else-if="tab === 'artifacts'" class="panel">
      <div v-if="resultSummary" class="result-summary">
        <span class="muted">结果摘要：</span>{{ resultSummary }}
      </div>
      <div v-else-if="resultNote" class="muted small result-note">{{ resultNote }}</div>
      <ArtifactsPanel :task-id="id" :events="events" :result="result" />
    </section>

    <section v-else-if="tab === 'calls'" class="panel">
      <div class="panel-tools">
        <span class="muted small">来自 /inspect：只含调用元数据，不含请求/响应正文或凭据</span>
        <span class="spacer"></span>
        <button class="btn small" type="button" :disabled="inspectLoading" @click="loadInspect">刷新</button>
      </div>
      <ErrorBanner :error="inspectError" />
      <CallsPanel :inspection="inspection" />
    </section>

    <section v-else class="panel">
      <div class="panel-tools">
        <span class="muted small">GET /tasks/{{ id }}/inspect 原始响应</span>
        <span class="spacer"></span>
        <button class="btn small" type="button" :disabled="inspectLoading" @click="loadInspect">刷新</button>
      </div>
      <ErrorBanner :error="inspectError" />
      <pre v-if="inspection" class="raw-json" data-testid="inspect-raw">{{ JSON.stringify(inspection, null, 2) }}</pre>
      <div v-else class="empty">{{ inspectLoading ? "加载中…" : "暂无数据" }}</div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ApiError } from "../api/client";
import type { Inspection, Task, TaskEvent, TaskResult } from "../api/client";
import type { EventStream, StreamState } from "../api/sse";
import { TERMINAL_EVENT } from "../api/sse";
import ArtifactsPanel from "../components/ArtifactsPanel.vue";
import CallsPanel from "../components/CallsPanel.vue";
import ErrorBanner from "../components/ErrorBanner.vue";
import EventTimeline from "../components/EventTimeline.vue";
import StatusBadge from "../components/StatusBadge.vue";
import TaskControls from "../components/TaskControls.vue";
import { isTerminal } from "../lib/controls";
import { shortId } from "../lib/format";
import { tasksHref } from "../lib/router";
import { useServices } from "../lib/services";
import { mergeEvents } from "../lib/timeline";

type TabId = "timeline" | "artifacts" | "calls" | "inspect";
const TABS: { id: TabId; label: string }[] = [
  { id: "timeline", label: "事件时间线" },
  { id: "artifacts", label: "产物与版本" },
  { id: "calls", label: "预算与调用" },
  { id: "inspect", label: "Inspect 原始" },
];

const props = defineProps<{ id: string }>();

const { api, watch: watchEvents } = useServices();

const task = ref<Task | null>(null);
const taskError = ref<unknown>(null);
const events = ref<TaskEvent[]>([]);
const streamState = ref<StreamState | "idle">("idle");
const streamError = ref<unknown>(null);
const result = ref<TaskResult | null>(null);
const resultNote = ref("");
const inspection = ref<Inspection | null>(null);
const inspectError = ref<unknown>(null);
const inspectLoading = ref(false);
const tab = ref<TabId>("timeline");

let stream: EventStream | null = null;
let pending: TaskEvent[] = [];
let flushQueued = false;
let refreshTimer: ReturnType<typeof setTimeout> | undefined;
let generation = 0;

const STREAM_LABELS: Record<string, string> = {
  idle: "未连接",
  connecting: "连接中",
  open: "实时",
  reconnecting: "重连中",
  terminal: "已结束",
  stopped: "已停止",
  closed: "已关闭",
};
const streamLabel = computed(() => STREAM_LABELS[streamState.value] ?? streamState.value);
const resultSummary = computed(() => result.value?.summary ?? "");

async function refreshTask(): Promise<void> {
  const gen = generation;
  try {
    const t = await api.getTask(props.id);
    if (gen !== generation) return;
    task.value = t;
    taskError.value = null;
    if (isTerminal(t.status) && !result.value) void loadResult();
  } catch (e) {
    if (gen === generation) taskError.value = e;
  }
}

/** 事件到达后合并刷新任务事实（控制按钮以服务端状态为准）。 */
function scheduleTaskRefresh(): void {
  if (refreshTimer !== undefined) return;
  refreshTimer = setTimeout(() => {
    refreshTimer = undefined;
    void refreshTask();
  }, 250);
}

async function loadResult(): Promise<void> {
  const gen = generation;
  try {
    const r = await api.getResult(props.id);
    if (gen !== generation) return;
    result.value = r;
    resultNote.value = "";
  } catch (e) {
    if (gen !== generation) return;
    if (e instanceof ApiError && (e.code === "not_ready" || e.code === "task_not_terminal")) {
      resultNote.value = e.code === "not_ready" ? "任务已结束但没有记录结果。" : "任务尚未结束。";
    } else {
      resultNote.value = "";
      taskError.value = e;
    }
  }
}

async function loadInspect(): Promise<void> {
  const gen = generation;
  inspectLoading.value = true;
  try {
    const r = await api.inspectTask(props.id);
    if (gen !== generation) return;
    inspection.value = r;
    inspectError.value = null;
  } catch (e) {
    if (gen === generation) inspectError.value = e;
  } finally {
    if (gen === generation) inspectLoading.value = false;
  }
}

function selectTab(id: TabId): void {
  tab.value = id;
  if ((id === "calls" || id === "inspect") && !inspection.value && !inspectLoading.value) void loadInspect();
}

function flush(): void {
  flushQueued = false;
  if (pending.length === 0) return;
  events.value = mergeEvents(events.value, pending);
  pending = [];
}

function onEvent(ev: TaskEvent): void {
  pending.push(ev);
  if (!flushQueued) {
    flushQueued = true;
    queueMicrotask(flush);
  }
  if (ev.type === TERMINAL_EVENT) {
    void refreshTask(); // 任务为终态时由 refreshTask 拉取 /result
    if (inspection.value || tab.value === "calls" || tab.value === "inspect") void loadInspect();
  } else {
    scheduleTaskRefresh();
  }
}

function stop(): void {
  stream?.close();
  stream = null;
  clearTimeout(refreshTimer);
  refreshTimer = undefined;
}

function start(): void {
  stop();
  generation++;
  const gen = generation;
  task.value = null;
  taskError.value = null;
  events.value = [];
  pending = [];
  streamError.value = null;
  result.value = null;
  resultNote.value = "";
  inspection.value = null;
  inspectError.value = null;
  streamState.value = "connecting";
  void refreshTask();
  if (tab.value === "calls" || tab.value === "inspect") void loadInspect();
  const s = watchEvents({
    taskId: props.id,
    cursor: 0,
    onEvent: (ev) => {
      if (gen === generation) onEvent(ev);
    },
    onState: (state) => {
      if (gen === generation) streamState.value = state;
    },
  });
  stream = s;
  void s.done.then((outcome) => {
    if (gen !== generation) return;
    if (outcome.kind === "error") streamError.value = outcome.error;
  });
}

watch(() => props.id, start, { immediate: true });

onBeforeUnmount(() => {
  generation++;
  stop();
});
</script>

<style scoped>
.back {
  font-size: 0.85rem;
}
.summary {
  display: flex;
  gap: 16px;
  align-items: flex-start;
  justify-content: space-between;
  flex-wrap: wrap;
}
.summary-main {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-width: 0;
}
.title-row {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.title-row .page-title {
  font-size: 1.1rem;
  word-break: break-all;
}
.facts {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 22px;
  margin: 0;
}
.facts div {
  display: flex;
  gap: 6px;
  align-items: baseline;
}
.facts dt {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: #6b7280;
}
.facts dd {
  margin: 0;
  font-size: 0.85rem;
}
.stream {
  font-size: 0.78rem;
  font-weight: 700;
}
.stream::before {
  content: "";
  display: inline-block;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  margin-right: 5px;
  background: #9ca3af;
}
.stream.open::before {
  background: #10b981;
}
.stream.connecting::before,
.stream.reconnecting::before {
  background: #f59e0b;
}
.stream.terminal::before {
  background: #7c3aed;
}
.stream.stopped::before {
  background: #ef4444;
}
.panel-tools {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
}
.small {
  font-size: 0.78rem;
}
.result-summary {
  margin-bottom: 12px;
  padding: 10px 12px;
  border-radius: 8px;
  background: #f5f3ff;
  font-size: 0.88rem;
}
.result-note {
  margin-bottom: 10px;
}
</style>
