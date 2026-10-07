<template>
  <section class="report-body" data-testid="report-body" aria-label="研究报告">
    <p v-if="loading" class="note">报告加载中…</p>
    <p v-else-if="loadError" class="note err" role="alert">{{ loadError }}</p>
    <!-- 报告经 marked → DOMPurify 清理后才插入（lib/markdown.ts）：不含脚本、事件属性、javascript: 链接与 iframe。 -->
    <!-- eslint-disable-next-line vue/no-v-html -->
    <article v-else-if="report" class="report markdown" v-html="report.html"></article>
  </section>
</template>

<script setup lang="ts">
// 对话中间的报告全文：与右侧"报告"标签共用 useReport 与同一个安全渲染（marked + DOMPurify）。
import type { TurnView } from "../../lib/chat";
import { useReport } from "../../lib/useReport";

const props = defineProps<{ turn: TurnView }>();
const { report, loading, loadError } = useReport(() => props.turn, () => true);
</script>

<style scoped>
.report-body {
  min-width: 0;
  padding: 2px 6px;
}
.note {
  margin: 0;
  color: #8c959f;
  font-size: 0.82rem;
}
.note.err {
  color: #b91c1c;
}
/* 与 SidePanel 的 .report 规则一致；对话栏中字号 15px、行高 1.75，宽度跟随对话栏 */
.report {
  font-size: 15px;
  line-height: 1.75;
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
</style>
