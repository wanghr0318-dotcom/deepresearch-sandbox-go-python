<template>
  <div class="step" :class="row.status">
    <div class="step-line">
      <button
        class="step-head"
        type="button"
        data-testid="step-head"
        :data-status="row.status"
        :aria-expanded="expanded ? 'true' : 'false'"
        @click="emit('toggle')"
      >
        <span class="caret" aria-hidden="true">{{ row.status === "running" ? "●" : expanded ? "▾" : "▸" }}</span>
        <span class="icon" aria-hidden="true">{{ ICONS[row.kind] }}</span>
        <span class="label" :class="{ think: row.kind === 'thinking' }">{{ label }}</span>
        <span v-if="row.status === 'error'" class="err-tag">失败</span>
      </button>
      <button v-if="row.raw" class="raw" type="button" aria-label="查看原始请求与响应" title="查看原始请求与响应" @click="emit('raw', row.raw)">
        ⟨/⟩
      </button>
    </div>

    <div v-if="expanded && hasBody" class="exp">
      <template v-if="row.kind === 'search' && row.results">
        <ol class="results">
          <li v-for="(r, i) in shownResults" :key="i" class="result" data-testid="search-result">
            <div class="r-line">
              <span class="r-n">{{ i + 1 }}.</span>
              <a v-if="isHttp(r.url)" class="r-title" :href="r.url" target="_blank" rel="noopener noreferrer">{{ r.title || r.url }}</a>
              <span v-else class="r-title">{{ r.title }}</span>
              <span v-if="r.site" class="r-site">— {{ r.site }}</span>
            </div>
            <div v-if="r.snippet" class="r-snippet">{{ r.snippet }}</div>
          </li>
        </ol>
        <button v-if="hidden > 0" class="more" type="button" data-action="more-results" @click="showAll = true">
          … 还有 {{ hidden }} 条（点击展开）
        </button>
      </template>
      <div v-if="row.kind !== 'search' && row.detail" class="detail" :class="{ mono: isCode }">{{ row.detail }}</div>
      <template v-if="isCode && row.text">
        <pre class="text mono" data-testid="code-output">{{ shownOutput }}</pre>
        <button v-if="hiddenLines > 0" class="more" type="button" data-action="more-lines" @click="showAll = true">
          … 还有 {{ hiddenLines }} 行（点击展开）
        </button>
      </template>
      <div v-else-if="row.text" class="text">{{ row.text }}</div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 一个 Agent 步骤（设计 §7）：默认折叠；搜索展开后逐行显示标题、站点、摘要；运行代码与运行命令展开后显示退出码与输出（前 12 行，可展开其余）；⟨/⟩ 只在有原始引用（经 Gateway 的调用：含工作区命令与外部工具）时出现。
// 只渲染面向用户的字段：从不显示行 id、子主题 id、原始响应的 sha。所有文本经插值（不使用 v-html）。
import { computed, ref, watch } from "vue";
import type { RawRef, StepKind, StepRow } from "../../lib/chat";

const props = defineProps<{ row: StepRow; expanded: boolean }>();
const emit = defineEmits<{ toggle: []; raw: [ref: RawRef] }>();

const ICONS: Record<StepKind, string> = {
  skill: "📖",
  thinking: "💭",
  ask: "❓",
  todo: "📝",
  search: "🔎",
  fetch: "🌐",
  source: "📚",
  subtopic: "🧭",
  code: "💻",
  shell: "⌨️",
  file: "📄",
  mcp: "🧩",
  tool: "🔧",
};

// 运行代码与运行命令（工作区）：等宽显示，输出默认前 12 行
const isCode = computed(() => props.row.kind === "code" || props.row.kind === "shell");

const FIRST_RESULTS = 5;
const FIRST_LINES = 12; // 运行代码：输出默认显示前 12 行
const showAll = ref(false);
watch(
  () => props.expanded,
  (v) => {
    if (!v) showAll.value = false;
  },
);

const label = computed(() => {
  const r = props.row;
  if (r.kind === "search" && r.detail) {
    if (r.status === "running") return `搜索网页「${r.detail}」…`;
    if (r.results && r.status === "done") return `搜索网页「${r.detail}」· ${r.results.length} 条结果`;
    return `搜索网页「${r.detail}」`;
  }
  return r.status === "running" ? `${r.title}…` : r.title;
});

const shownResults = computed(() => {
  const list = props.row.results ?? [];
  return showAll.value ? list : list.slice(0, FIRST_RESULTS);
});
const hidden = computed(() => (props.row.results?.length ?? 0) - shownResults.value.length);
const outputLines = computed(() => (props.row.text ?? "").split("\n"));
const shownLines = computed(() => (showAll.value ? outputLines.value : outputLines.value.slice(0, FIRST_LINES)));
const shownOutput = computed(() => shownLines.value.join("\n"));
const hiddenLines = computed(() => outputLines.value.length - shownLines.value.length);
const hasBody = computed(() => {
  const r = props.row;
  return (r.results?.length ?? 0) > 0 || !!r.text || (r.kind !== "search" && !!r.detail);
});

function isHttp(url: string): boolean {
  return /^https?:\/\//i.test(url);
}
</script>

<style scoped>
.step {
  display: flex;
  flex-direction: column;
}
.step-line {
  display: flex;
  align-items: center;
  gap: 4px;
}
.step-head {
  flex: 1;
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px 6px;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #424a53;
  font: inherit;
  font-size: 0.86rem;
  text-align: left;
  cursor: pointer;
}
.step-head:hover {
  background: #f6f8fa;
}
.step-head:focus-visible,
.raw:focus-visible,
.more:focus-visible {
  outline: 2px solid #7c3aed;
  outline-offset: 1px;
}
.caret {
  width: 12px;
  flex-shrink: 0;
  color: #8c959f;
  font-size: 0.75rem;
}
.running .caret {
  color: #9a6700;
  animation: blink 1.2s ease-in-out infinite;
}
.running .step-head {
  color: #9a6700;
}
.error .step-head {
  color: #b91c1c;
}
.icon {
  flex-shrink: 0;
}
.label {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.think {
  font-style: italic;
  color: #656d76;
}
.err-tag {
  flex-shrink: 0;
  font-size: 0.72rem;
  padding: 0 6px;
  border-radius: 8px;
  background: #fee2e2;
  color: #991b1b;
}
.raw {
  flex-shrink: 0;
  border: 0;
  background: transparent;
  color: #0969da;
  font: inherit;
  font-size: 0.74rem;
  padding: 2px 6px;
  border-radius: 6px;
  cursor: pointer;
}
.raw:hover {
  background: #eef4fb;
}
.exp {
  margin: 0 0 4px 24px;
  border-left: 2px solid #eaeef2;
  padding: 2px 10px;
  font-size: 0.8rem;
  color: #424a53;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.results {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.r-line {
  display: flex;
  gap: 6px;
  min-width: 0;
}
.r-n {
  color: #8c959f;
  flex-shrink: 0;
}
.r-title {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #1f2328;
  text-decoration: none;
}
a.r-title:hover {
  color: #6d28d9;
  text-decoration: underline;
}
.r-site {
  color: #656d76;
  flex-shrink: 0;
}
.r-snippet {
  margin-left: 18px;
  color: #656d76;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.more {
  align-self: flex-start;
  border: 0;
  background: transparent;
  color: #8c959f;
  font: inherit;
  padding: 0;
  cursor: pointer;
}
.more:hover {
  color: #6d28d9;
}
.detail {
  color: #656d76;
  word-break: break-all;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.78rem;
}
pre.text {
  margin: 0;
}
.text {
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 16em;
  overflow: auto;
}
@keyframes blink {
  50% {
    opacity: 0.35;
  }
}
</style>
