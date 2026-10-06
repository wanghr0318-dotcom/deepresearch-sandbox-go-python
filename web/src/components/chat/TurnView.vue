<template>
  <div class="turn" :data-status="turn.status">
    <div class="t-head">
      <span v-if="turn.route" class="route" :class="turn.route" data-testid="route">{{ turn.route === "research" ? "深度研究" : "直接回答" }}</span>
      <span v-if="statusText" class="t-status">{{ statusText }}</span>
      <span class="spacer"></span>
      <button v-if="turn.route === 'research'" class="link-btn" type="button" data-action="view-progress" @click="emit('panel', 'progress')">
        查看进度
      </button>
      <button
        v-if="turn.status === 'running'"
        class="btn small stop-btn"
        type="button"
        data-action="stop"
        :disabled="busy"
        @click="emit('action', turn.turnId, 'stop')"
      >
        ■ 停止
      </button>
    </div>

    <div v-if="turn.steps.length" class="steps">
      <template v-for="(row, i) in turn.steps" :key="row.id">
        <div v-if="row.subtopic && row.subtopic !== turn.steps[i - 1]?.subtopic" class="sub-heading" data-testid="subtopic-heading">
          {{ row.subtopic }}
        </div>
        <StepRow :row="row" :expanded="isExpanded(row)" @toggle="toggle(row)" @raw="openRaw" />
      </template>
    </div>

    <QuestionCard
      v-if="turn.question"
      :question="turn.question"
      :disabled="busy || turn.status !== 'awaiting_input'"
      @answer="(a) => emit('answer', turn.turnId, a)"
    />

    <div v-if="turn.reply" class="reply" data-testid="reply">{{ turn.reply }}</div>

    <StopCard
      :stop="turn.stop"
      :status="turn.status"
      :can-restore="turn.canRestore"
      :busy="busy"
      @continue="emit('action', turn.turnId, 'continue')"
      @finish="emit('action', turn.turnId, 'finish')"
      @restore="emit('restore', turn.turnId)"
    />

    <div v-if="turn.report" class="report-bar" data-testid="report-bar">
      <span class="r-icon" aria-hidden="true">📄</span>
      <span class="r-text">
        <strong>报告已生成</strong>
        <span class="r-title">{{ turn.report.title }}</span>
        <span v-if="turn.report.partial" class="partial">部分研究</span>
      </span>
      <span class="spacer"></span>
      <button class="btn primary small" type="button" data-action="view-report" @click="emit('panel', 'report')">查看报告</button>
      <p v-if="turn.report.toolBudgetReached || turn.report.note" class="r-note">
        <template v-if="turn.report.toolBudgetReached">本轮的工具调用额度已用完，报告基于已收集的资料。</template>
        {{ turn.report.note }}
      </p>
    </div>

    <div v-if="turn.error" class="t-error" role="alert">{{ turn.error }}</div>

    <RawDialog :turn-id="turn.turnId" :raw-ref="rawRef" @close="rawRef = null" />
  </div>
</template>

<script setup lang="ts">
// 一轮对话中 AI 一侧的渲染：路径标识、步骤行（按子主题分组）、流式回复（纯文本）、提问卡、停止卡、报告提示条与失败提示。
// 进行中的步骤默认展开、其余折叠；用户点击后以用户的选择为准。
import { computed, ref } from "vue";
import type { Answer, TurnAction } from "../../api/chat";
import type { RawRef, StepRow as StepRowData, TurnView } from "../../lib/chat";
import QuestionCard from "./QuestionCard.vue";
import RawDialog from "./RawDialog.vue";
import StepRow from "./StepRow.vue";
import StopCard from "./StopCard.vue";

const props = defineProps<{ turn: TurnView; busy: boolean }>();
const emit = defineEmits<{
  answer: [turnId: string, answers: Answer[]];
  action: [turnId: string, action: TurnAction];
  restore: [turnId: string];
  panel: [tab: "progress" | "sources" | "report"];
}>();

const overrides = ref(new Map<string, boolean>());
const rawRef = ref<RawRef | null>(null);

function isExpanded(row: StepRowData): boolean {
  return overrides.value.get(row.id) ?? row.status === "running";
}

function toggle(row: StepRowData): void {
  const next = new Map(overrides.value);
  next.set(row.id, !isExpanded(row));
  overrides.value = next;
}

function openRaw(r: RawRef): void {
  rawRef.value = r;
}

const statusText = computed(() => {
  const t = props.turn;
  switch (t.status) {
    case "queued":
      return "排队中…";
    case "stopping":
      return "正在停止…";
    case "awaiting_input":
      return "等待你的回答";
    case "running":
      return !t.steps.length && !t.reply ? "思考中…" : "";
    default:
      return "";
  }
});
</script>

<style scoped>
.turn {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 0;
  font-size: 0.92rem;
  color: #1f2328;
}
.t-head {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 24px;
}
.spacer {
  flex: 1;
}
.route {
  font-size: 0.72rem;
  font-weight: 700;
  padding: 1px 8px;
  border-radius: 10px;
  background: #f3f4f6;
  color: #4b5563;
}
.route.research {
  background: #ede9fe;
  color: #5b21b6;
}
.t-status {
  font-size: 0.78rem;
  color: #6b7280;
}
.link-btn {
  border: 0;
  background: transparent;
  color: #6d28d9;
  font: inherit;
  font-size: 0.78rem;
  cursor: pointer;
  padding: 2px 4px;
}
.link-btn:hover {
  text-decoration: underline;
}
.stop-btn {
  border-radius: 14px;
}
.steps {
  display: flex;
  flex-direction: column;
  gap: 1px;
}
.sub-heading {
  margin: 6px 0 2px 6px;
  font-size: 0.74rem;
  font-weight: 700;
  color: #6b7280;
  letter-spacing: 0.02em;
}
.reply {
  white-space: pre-wrap;
  word-break: break-word;
  line-height: 1.7;
  padding: 2px 6px;
}
.report-bar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  padding: 9px 12px;
  border-radius: 10px;
  border: 1px solid #ddd6fe;
  background: #f5f3ff;
}
.r-text {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  min-width: 0;
}
.r-title {
  color: #4b5563;
  font-size: 0.85rem;
  word-break: break-word;
}
.partial {
  font-size: 0.72rem;
  font-weight: 700;
  padding: 1px 7px;
  border-radius: 10px;
  background: #fef3c7;
  color: #92400e;
}
.r-note {
  flex-basis: 100%;
  margin: 0;
  font-size: 0.78rem;
  color: #6b7280;
}
.t-error {
  padding: 8px 12px;
  border-radius: 8px;
  background: #fef2f2;
  border: 1px solid #fecaca;
  color: #991b1b;
  font-size: 0.86rem;
}
</style>
