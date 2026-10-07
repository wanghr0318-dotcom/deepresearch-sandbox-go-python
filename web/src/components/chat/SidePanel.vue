<template>
  <aside class="side-panel" aria-label="研究面板">
    <div class="tabs" role="tablist" aria-label="研究面板">
      <button
        v-for="t in TABS"
        :id="`side-tab-${t.key}`"
        :key="t.key"
        class="s-tab"
        :class="{ on: tab === t.key }"
        type="button"
        role="tab"
        :aria-selected="tab === t.key ? 'true' : 'false'"
        aria-controls="side-tabpanel"
        :data-tab="t.key"
        @click="emit('update:tab', t.key)"
      >
        {{ t.key === "sources" && turn?.sources.length ? `${t.name} ${turn.sources.length}` : t.name }}
      </button>
    </div>

    <div id="side-tabpanel" class="body" role="tabpanel" :aria-labelledby="`side-tab-${tab}`">
      <p v-if="!turn" class="empty-note">开始一次深度研究后，这里会显示进度、来源与报告。</p>

      <template v-else-if="tab === 'progress'">
        <ul v-if="turn.todo.length" class="todo">
          <li v-for="(t, i) in turn.todo" :key="i" :class="t.status" data-testid="todo-item" :data-status="t.status">
            <span class="mark" aria-hidden="true">{{ TODO_MARK[t.status] }}</span>
            <span class="t-title">{{ t.title }}</span>
            <span v-if="t.budgetShare" class="share">约 {{ t.budgetShare }} 次调用</span>
            <span class="sr-only">（{{ TODO_LABEL[t.status] }}）</span>
          </li>
        </ul>
        <p v-else class="empty-note">还没有待办清单。</p>

        <template v-if="budget">
          <div
            class="bar"
            role="progressbar"
            aria-label="工具调用额度"
            :aria-valuenow="budget.used"
            aria-valuemin="0"
            :aria-valuemax="budget.limit"
          >
            <i :style="{ width: `${budgetPct}%` }" :class="{ full: budget.used >= budget.limit }"></i>
          </div>
          <div class="bar-text">工具调用 {{ budget.used }} / {{ budget.limit }}</div>
        </template>
      </template>

      <template v-else-if="tab === 'sources'">
        <ol v-if="turn.sources.length" class="sources">
          <li v-for="s in turn.sources" :key="s.n" data-testid="source">
            <a v-if="isHttp(s.url)" :href="s.url" target="_blank" rel="noopener noreferrer">[{{ s.n }}] {{ s.title }}<template v-if="s.site"> · {{ s.site }}</template></a>
            <span v-else>[{{ s.n }}] {{ s.title }}<template v-if="s.site"> · {{ s.site }}</template></span>
          </li>
        </ol>
        <p v-else class="empty-note">还没有来源。</p>
      </template>

      <template v-else>
        <p v-if="!turn.report" class="empty-note">报告还没有生成。</p>
        <template v-else>
          <div class="r-tools">
            <span class="r-title">{{ turn.report.title }}</span>
            <span v-if="turn.report.partial" class="partial">部分研究</span>
            <span class="spacer"></span>
            <button class="btn small" type="button" data-action="download-report" :disabled="!report" @click="download">下载</button>
          </div>
          <p v-if="loading" class="empty-note">报告加载中…</p>
          <p v-else-if="loadError" class="fail" role="alert">{{ loadError }}</p>
          <!-- 报告经 marked → DOMPurify 清理后才插入（lib/markdown.ts）：不含脚本、事件属性、javascript: 链接与 iframe。 -->
          <!-- eslint-disable-next-line vue/no-v-html -->
          <article v-else-if="report" class="report markdown" data-testid="report" v-html="report.html"></article>
        </template>
      </template>
    </div>
  </aside>
</template>

<script setup lang="ts">
// 右侧面板（布局 B）：进度（待办清单 + 30 次工具调用额度条）、来源（编号列表，新标签页打开）、报告（安全渲染 + 下载）。
// 报告只在切到"报告"标签时读取，同一版本只读一次。
import { computed } from "vue";
import type { TodoItem, TurnView } from "../../lib/chat";
import { reportFilename } from "../../lib/research";
import { useReport } from "../../lib/useReport";
import { useUserServices } from "../../lib/userServices";

type Tab = "progress" | "sources" | "report";

const props = defineProps<{ turn?: TurnView; tab: Tab }>();
const emit = defineEmits<{ "update:tab": [tab: Tab] }>();

const { saveBlob } = useUserServices();

const TABS: { key: Tab; name: string }[] = [
  { key: "progress", name: "进度" },
  { key: "sources", name: "来源" },
  { key: "report", name: "报告" },
];
const TODO_MARK: Record<TodoItem["status"], string> = { done: "✓", in_progress: "●", pending: "○", skipped: "–" };
const TODO_LABEL: Record<TodoItem["status"], string> = { done: "已完成", in_progress: "进行中", pending: "未开始", skipped: "已跳过" };

const budget = computed(() => {
  const t = props.turn;
  const b = t?.budget ?? t?.stop?.budget;
  return b && b.limit > 0 ? b : undefined;
});
const budgetPct = computed(() => {
  const b = budget.value;
  return b ? Math.min(100, Math.max(0, (b.used / b.limit) * 100)) : 0;
});

const { report, loading, loadError } = useReport(() => props.turn, () => props.tab === "report");

function download(): void {
  const t = props.turn;
  if (!report.value || !t?.report) return;
  saveBlob(report.value.blob, reportFilename(t.report.title, t.turnId));
}

function isHttp(url: string): boolean {
  return /^https?:\/\//i.test(url);
}
</script>

<style scoped>
.side-panel {
  display: flex;
  flex-direction: column;
  min-width: 0;
  height: 100%;
  background: #fbfcfd;
  font-size: 0.86rem;
  color: #1f2328;
}
.tabs {
  display: flex;
  gap: 14px;
  padding: 10px 14px 0;
  border-bottom: 1px solid #e5e7eb;
}
.s-tab {
  border: 0;
  border-bottom: 2px solid transparent;
  background: transparent;
  padding: 4px 0 7px;
  font: inherit;
  font-weight: 700;
  color: #6b7280;
  cursor: pointer;
}
.s-tab.on {
  color: #6d28d9;
  border-bottom-color: #7c3aed;
}
.s-tab:focus-visible {
  outline: 2px solid #7c3aed;
  outline-offset: 2px;
}
.body {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 12px 14px 18px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.empty-note {
  margin: 0;
  color: #8c959f;
  font-size: 0.82rem;
}
.todo {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 5px;
}
.todo li {
  display: flex;
  align-items: baseline;
  gap: 6px;
  color: #8c959f;
}
.todo li.done {
  color: #1a7f37;
}
.todo li.in_progress {
  color: #9a6700;
  font-weight: 600;
}
.todo li.skipped .t-title {
  text-decoration: line-through;
}
.mark {
  width: 12px;
  flex-shrink: 0;
  text-align: center;
}
.t-title {
  min-width: 0;
  word-break: break-word;
}
.share {
  flex-shrink: 0;
  font-size: 0.72rem;
  color: #8c959f;
  font-weight: 400;
}
.bar {
  margin-top: 8px;
  height: 6px;
  border-radius: 3px;
  background: #eaeef2;
  overflow: hidden;
}
.bar i {
  display: block;
  height: 100%;
  border-radius: 3px;
  background: #7c3aed;
  transition: width 0.3s ease;
}
.bar i.full {
  background: #d97706;
}
.bar-text {
  font-size: 0.76rem;
  color: #656d76;
  font-variant-numeric: tabular-nums;
}
.sources {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.sources a,
.sources span {
  font-size: 0.82rem;
  color: #0969da;
  text-decoration: none;
  word-break: break-word;
}
.sources span {
  color: #424a53;
}
.sources a:hover {
  text-decoration: underline;
}
.r-tools {
  display: flex;
  align-items: center;
  gap: 8px;
  padding-bottom: 8px;
  border-bottom: 1px solid #f3f4f6;
}
.r-title {
  font-weight: 700;
  min-width: 0;
  word-break: break-word;
}
.spacer {
  flex: 1;
}
.partial {
  flex-shrink: 0;
  font-size: 0.72rem;
  font-weight: 700;
  padding: 1px 7px;
  border-radius: 10px;
  background: #fef3c7;
  color: #92400e;
}
.fail {
  margin: 0;
  color: #b91c1c;
}
.report {
  font-size: 0.88rem;
  line-height: 1.7;
  color: #1f2937;
  word-break: break-word;
}
.report :deep(h1) {
  font-size: 1.2rem;
  margin: 0.4em 0 0.5em;
}
.report :deep(h2) {
  font-size: 1.04rem;
  margin: 1.2em 0 0.4em;
  padding-bottom: 0.2em;
  border-bottom: 1px solid #f3f4f6;
}
.report :deep(h3) {
  font-size: 0.95rem;
  margin: 1em 0 0.3em;
}
.report :deep(p),
.report :deep(ul),
.report :deep(ol) {
  margin: 0.5em 0;
}
.report :deep(a) {
  color: #6d28d9;
  word-break: break-all;
}
.report :deep(pre) {
  background: #f3f4f6;
  padding: 8px 10px;
  border-radius: 8px;
  overflow: auto;
}
.report :deep(code) {
  font-family: "JetBrains Mono", Consolas, monospace;
  font-size: 0.85em;
}
.report :deep(blockquote) {
  margin: 0.6em 0;
  padding: 2px 12px;
  border-left: 3px solid #ddd6fe;
  color: #4b5563;
}
.report :deep(table) {
  border-collapse: collapse;
  display: block;
  overflow-x: auto;
}
.report :deep(th),
.report :deep(td) {
  border: 1px solid #e5e7eb;
  padding: 4px 8px;
}
.report :deep(img) {
  max-width: 100%;
}
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
}
</style>
