<template>
  <div class="research">
    <a class="link back" :href="homeHref()">← 我的研究</a>

    <div v-if="notFound" class="panel lost">
      <h1 class="r-title">找不到这项研究</h1>
      <p class="muted">它可能不存在，或者不属于当前账号。</p>
      <a class="btn primary" :href="homeHref()">返回我的研究</a>
    </div>

    <template v-else>
      <header class="r-head">
        <div class="r-head-main">
          <h1 class="r-title" data-testid="topic">{{ title }}</h1>
          <div class="r-meta">
            <StatusBadge v-if="task" :status="task.status" :label="statusLabel(task.status)" />
            <span v-if="task?.created_at" class="created" data-testid="created">创建于 {{ formatCreated(task.created_at) }}</span>
            <span v-if="live" class="live" :class="streamState" data-testid="live">{{ live }}</span>
          </div>
        </div>
        <button
          v-if="canCancel(task)"
          class="btn danger"
          type="button"
          data-action="cancel"
          :disabled="cancelling"
          @click="cancel"
        >
          {{ cancelling ? "正在取消…" : "取消研究" }}
        </button>
      </header>

      <div v-if="error" class="user-error" role="alert">{{ error }}</div>

      <section class="panel stages" aria-label="研究进度">
        <ol class="stepper">
          <li v-for="(s, i) in stages" :key="s.key" class="step" :class="s.state" :data-stage="s.key" :data-state="s.state">
            <span class="dot">
              <svg v-if="s.state === 'done'" viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8.5l3 3 6-7" /></svg>
              <span v-else>{{ i + 1 }}</span>
            </span>
            <span class="step-text">
              <span class="step-name">{{ s.name }}</span>
              <span class="step-desc">{{ s.desc }}</span>
            </span>
          </li>
        </ol>
        <div class="stats">
          <div class="stat">
            <span class="stat-num" data-testid="subtasks">{{ progress.subtasksDone }}</span>
            <span class="stat-label">已完成子任务</span>
          </div>
          <div class="stat">
            <span class="stat-num" data-testid="evidence">{{ evidenceCount ?? "—" }}</span>
            <span class="stat-label">{{ evidenceCount === null ? "证据（报告生成后统计）" : "条证据" }}</span>
          </div>
        </div>
      </section>

      <section v-if="reportHtml" class="panel report-panel">
        <div class="report-tools">
          <h2 class="report-heading">研究报告</h2>
          <span class="spacer"></span>
          <button class="btn primary" type="button" data-action="download" @click="download">下载报告（Markdown）</button>
        </div>
        <!-- 报告经 marked → DOMPurify 清理后才插入（lib/markdown.ts）：不含脚本、事件属性、javascript: 链接与 iframe。 -->
        <!-- eslint-disable-next-line vue/no-v-html -->
        <article class="report markdown" data-testid="report" v-html="reportHtml"></article>
      </section>
      <section v-else-if="reportBlob" class="panel report-panel">
        <div class="report-tools">
          <h2 class="report-heading">研究报告</h2>
          <span class="spacer"></span>
          <button class="btn primary" type="button" data-action="download" @click="download">下载报告</button>
        </div>
        <p class="muted">该报告格式不支持在页面中预览，请下载后查看。</p>
      </section>
      <section v-else-if="endNote" class="panel end-note">{{ endNote }}</section>
      <section v-else-if="!terminal" class="panel waiting">
        <span class="spinner" aria-hidden="true"></span>
        <span>{{ waitingText }}</span>
      </section>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ApiError } from "../api/client";
import type { Task, TaskEvent } from "../api/client";
import type { EventStream, StreamState } from "../api/sse";
import { TERMINAL_EVENT } from "../api/sse";
import StatusBadge from "../components/StatusBadge.vue";
import { renderMarkdown } from "../lib/markdown";
import { previewKindForDownload } from "../lib/media";
import {
  canCancel,
  countEvidence,
  deriveProgress,
  formatCreated,
  isTerminalStatus,
  isUnauthorized,
  reportFilename,
  reportTitle,
  statusLabel,
  userErrorMessage,
} from "../lib/research";
import type { StageState } from "../lib/research";
import { homeHref } from "../lib/router";
import { mergeEvents } from "../lib/timeline";
import { useUserServices } from "../lib/userServices";

const props = defineProps<{ id: string }>();
const emit = defineEmits<{ unauthorized: [] }>();

const { api, watch: watchEvents, saveBlob } = useUserServices();

const task = ref<Task | null>(null);
const events = ref<TaskEvent[]>([]);
const streamState = ref<StreamState | "idle">("idle");
const error = ref("");
const notFound = ref(false);
const cancelling = ref(false);
const reportHtml = ref("");
const reportBlob = ref<Blob | null>(null);
const reportText = ref("");
const endNote = ref("");
const reportTopic = ref("");

let stream: EventStream | null = null;
let generation = 0;
let refreshTimer: ReturnType<typeof setTimeout> | undefined;
let reportLoading = false;

const terminal = computed(() => isTerminalStatus(task.value?.status));
const progress = computed(() => deriveProgress(events.value, task.value?.status));
const evidenceCount = computed(() => (reportText.value ? countEvidence(reportText.value) : null));
// 主题来自服务端任务视图（spec.topic）；缺失时退回报告的一级标题。
const topic = computed(() => task.value?.topic?.trim() || reportTopic.value);
const title = computed(() => topic.value || (task.value ? `研究 ${props.id.slice(0, 8)}` : "加载中…"));

const LIVE: Record<string, string> = {
  connecting: "连接中",
  open: "实时更新中",
  reconnecting: "网络波动，正在重连",
};
const live = computed(() => (terminal.value ? "" : (LIVE[streamState.value] ?? "")));

const STAGE_DESC: Record<string, Record<StageState, string>> = {
  plan: { pending: "等待开始", active: "正在拟定研究计划", done: "研究计划已拟定", stopped: "未完成" },
  tasks: { pending: "等待计划完成", active: "正在检索与总结资料", done: "资料收集完成", stopped: "未完成" },
  report: { pending: "等待资料收集", active: "正在撰写报告", done: "报告已生成", stopped: "未完成" },
};

const stages = computed(() => {
  const p = progress.value;
  const tasksDesc =
    p.tasks === "active" || (p.tasks === "stopped" && p.subtasksDone > 0)
      ? `已完成 ${p.subtasksDone} 个子任务`
      : STAGE_DESC.tasks![p.tasks];
  return [
    { key: "plan", name: "研究计划", state: p.plan, desc: STAGE_DESC.plan![p.plan] },
    { key: "tasks", name: "子任务", state: p.tasks, desc: tasksDesc },
    { key: "report", name: "研究报告", state: p.report, desc: STAGE_DESC.report![p.report] },
  ];
});

const waitingText = computed(() => {
  if (task.value?.status === "queued") return "排队中，马上开始…";
  if (task.value?.status === "cancelling" || task.value?.desired === "cancel") return "正在取消…";
  return "研究进行中，完成后报告会显示在这里。";
});

function fail(e: unknown): void {
  if (isUnauthorized(e)) {
    emit("unauthorized");
    return;
  }
  if (e instanceof ApiError && e.code === "task_not_found") {
    notFound.value = true;
    stop();
    return;
  }
  error.value = userErrorMessage(e);
}

async function refreshTask(): Promise<void> {
  const gen = generation;
  try {
    const t = await api.getTask(props.id);
    if (gen !== generation) return;
    task.value = t;
    error.value = "";
    if (isTerminalStatus(t.status)) void loadReport();
  } catch (e) {
    if (gen === generation) fail(e);
  }
}

function scheduleRefresh(): void {
  if (refreshTimer !== undefined) return;
  refreshTimer = setTimeout(() => {
    refreshTimer = undefined;
    void refreshTask();
  }, 250);
}

async function loadReport(): Promise<void> {
  if (reportLoading || reportBlob.value || endNote.value) return;
  reportLoading = true;
  const gen = generation;
  try {
    let result;
    try {
      result = await api.getResult(props.id);
    } catch (e) {
      if (e instanceof ApiError && (e.code === "not_ready" || e.code === "task_not_terminal")) {
        if (gen === generation) endNote.value = endText();
        return;
      }
      throw e;
    }
    if (gen !== generation) return;
    // 只取已保存（有版本）的输出；DeepResearch 的报告产物 ID 为 "report"。
    const saved = result.outputs.filter((o) => o.version !== undefined);
    const out = saved.find((o) => o.artifact_id === "report") ?? saved[0];
    if (!out || out.version === undefined) {
      endNote.value = endText();
      return;
    }
    const dl = await api.downloadArtifact(props.id, out.artifact_id, out.version);
    if (gen !== generation) return;
    reportBlob.value = dl.blob;
    if (previewKindForDownload(dl.contentType, dl.contentDisposition) === "markdown" || /text\/plain/i.test(dl.contentType)) {
      const text = await dl.blob.text();
      if (gen !== generation) return;
      reportText.value = text;
      reportHtml.value = renderMarkdown(text);
      const h1 = reportTitle(text);
      if (h1) reportTopic.value = h1;
    }
  } catch (e) {
    if (gen === generation) fail(e);
  } finally {
    reportLoading = false;
  }
}

function endText(): string {
  switch (task.value?.status) {
    case "cancelled":
      return "这项研究已取消。";
    case "failed":
      return "这项研究未能完成，请换个主题或稍后再试。";
    default:
      return "研究已结束，但没有生成报告。";
  }
}

function download(): void {
  if (!reportBlob.value) return;
  saveBlob(reportBlob.value, reportFilename(topic.value, props.id));
}

async function cancel(): Promise<void> {
  if (cancelling.value) return;
  cancelling.value = true;
  try {
    await api.cancelTask(props.id);
    await refreshTask();
  } catch (e) {
    if (e instanceof ApiError && (e.code === "task_ended" || e.code === "cancel_pending")) await refreshTask();
    else fail(e);
  } finally {
    cancelling.value = false;
  }
}

function onEvent(ev: TaskEvent): void {
  events.value = mergeEvents(events.value, [ev]);
  if (ev.type === TERMINAL_EVENT) void refreshTask();
  else scheduleRefresh();
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
  events.value = [];
  error.value = "";
  notFound.value = false;
  reportHtml.value = "";
  reportBlob.value = null;
  reportText.value = "";
  endNote.value = "";
  reportLoading = false;
  reportTopic.value = "";
  streamState.value = "connecting";
  void refreshTask();
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
    if (gen !== generation || outcome.kind !== "error") return;
    fail(outcome.error);
  });
}

watch(() => props.id, start, { immediate: true });

onBeforeUnmount(() => {
  generation++;
  stop();
});
</script>

<style scoped>
.research {
  width: 100%;
  max-width: 900px;
  margin: 0 auto;
  padding: 28px 24px 56px;
  display: flex;
  flex-direction: column;
  gap: 18px;
}
.back {
  font-size: 0.85rem;
}
.r-head {
  display: flex;
  align-items: flex-start;
  gap: 16px;
}
.r-head-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.r-title {
  margin: 0;
  font-size: 1.55rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: #111827;
  word-break: break-word;
}
.r-meta {
  display: flex;
  align-items: center;
  gap: 12px;
}
.created {
  font-size: 0.78rem;
  color: #9ca3af;
}
.live {
  font-size: 0.78rem;
  color: #6b7280;
  font-weight: 600;
}
.live::before {
  content: "";
  display: inline-block;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  margin-right: 6px;
  background: #f59e0b;
}
.live.open::before {
  background: #10b981;
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.6);
}
.stages {
  display: flex;
  flex-direction: column;
  gap: 18px;
  padding: 20px 22px;
}
.stepper {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
}
.step {
  position: relative;
  display: flex;
  align-items: flex-start;
  gap: 10px;
}
.dot {
  width: 30px;
  height: 30px;
  flex-shrink: 0;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.82rem;
  font-weight: 700;
  background: #f3f4f6;
  color: #9ca3af;
  border: 2px solid #e5e7eb;
}
.dot svg {
  width: 16px;
  height: 16px;
  fill: none;
  stroke: #ffffff;
  stroke-width: 2.2;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.step.done .dot {
  background: #7c3aed;
  border-color: #7c3aed;
}
.step.active .dot {
  background: #ffffff;
  border-color: #7c3aed;
  color: #6d28d9;
  animation: pulse 1.6s ease-in-out infinite;
}
.step.stopped .dot {
  background: #fef2f2;
  border-color: #fecaca;
  color: #b91c1c;
}
.step-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}
.step-name {
  font-weight: 700;
  font-size: 0.92rem;
  color: #111827;
}
.step.pending .step-name {
  color: #9ca3af;
}
.step-desc {
  font-size: 0.78rem;
  color: #6b7280;
}
.stats {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}
.stat {
  flex: 1;
  min-width: 160px;
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 12px 14px;
  border-radius: 10px;
  background: #f9fafb;
  border: 1px solid #f3f4f6;
}
.stat-num {
  font-size: 1.5rem;
  font-weight: 700;
  color: #6d28d9;
  font-variant-numeric: tabular-nums;
}
.stat-label {
  font-size: 0.8rem;
  color: #6b7280;
}
.report-panel {
  padding: 20px 26px 28px;
}
.report-tools {
  display: flex;
  align-items: center;
  gap: 10px;
  padding-bottom: 12px;
  margin-bottom: 8px;
  border-bottom: 1px solid #f3f4f6;
}
.report-heading {
  margin: 0;
  font-size: 1rem;
  font-weight: 700;
}
.report {
  font-size: 0.96rem;
  line-height: 1.75;
  color: #1f2937;
  word-break: break-word;
}
.report :deep(h1) {
  font-size: 1.45rem;
  margin: 0.6em 0 0.5em;
  letter-spacing: -0.02em;
}
.report :deep(h2) {
  font-size: 1.15rem;
  margin: 1.4em 0 0.5em;
  padding-bottom: 0.25em;
  border-bottom: 1px solid #f3f4f6;
}
.report :deep(h3) {
  font-size: 1rem;
  margin: 1.2em 0 0.4em;
}
.report :deep(p),
.report :deep(ul),
.report :deep(ol) {
  margin: 0.6em 0;
}
.report :deep(li) {
  margin: 0.25em 0;
}
.report :deep(a) {
  color: #6d28d9;
  word-break: break-all;
}
.report :deep(pre) {
  background: #f3f4f6;
  padding: 10px 12px;
  border-radius: 8px;
  overflow: auto;
}
.report :deep(code) {
  font-family: "JetBrains Mono", Consolas, monospace;
  font-size: 0.85em;
}
.report :deep(blockquote) {
  margin: 0.8em 0;
  padding: 2px 14px;
  border-left: 3px solid #ddd6fe;
  color: #4b5563;
}
.report :deep(table) {
  border-collapse: collapse;
}
.report :deep(th),
.report :deep(td) {
  border: 1px solid #e5e7eb;
  padding: 4px 8px;
}
.report :deep(img) {
  max-width: 100%;
}
.waiting {
  display: flex;
  align-items: center;
  gap: 12px;
  color: #4b5563;
  font-size: 0.9rem;
}
.spinner {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  border: 2px solid #ddd6fe;
  border-top-color: #7c3aed;
  animation: spin 0.9s linear infinite;
}
.end-note {
  color: #4b5563;
  font-size: 0.92rem;
}
.lost {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 10px;
  padding: 26px;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
@keyframes pulse {
  0%,
  100% {
    box-shadow: 0 0 0 0 rgba(124, 58, 237, 0.35);
  }
  50% {
    box-shadow: 0 0 0 6px rgba(124, 58, 237, 0);
  }
}
@media (max-width: 640px) {
  .stepper {
    grid-template-columns: 1fr;
  }
  .r-head {
    flex-direction: column;
  }
}
</style>
