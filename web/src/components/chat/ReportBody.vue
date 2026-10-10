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
/* 报告正文的共用样式见 report.css；对话栏中字号 15px、行高 1.75，宽度跟随对话栏 */
.report {
  font-size: 15px;
  line-height: 1.75;
}
</style>

<style scoped src="./report.css"></style>
