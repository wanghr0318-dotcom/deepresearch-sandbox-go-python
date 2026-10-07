<template>
  <div class="turn" :data-status="turn.status">
    <div class="t-head">
      <span v-if="label" class="route" :class="turn.route ?? 'research'" data-testid="route">{{ label }}</span>
      <span v-if="statusText" class="t-status">{{ statusText }}</span>
      <span class="spacer"></span>
      <button v-if="isResearch" class="link-btn" type="button" data-action="view-progress" @click="emit('panel', 'progress')">
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

    <button
      v-if="turn.steps.length && finished"
      class="chain-toggle"
      type="button"
      data-testid="chain-toggle"
      :aria-expanded="chainOpen ? 'true' : 'false'"
      @click="chainOpen = !chainOpen"
    >
      <span aria-hidden="true">{{ chainOpen ? "▾" : "▸" }}</span> {{ chainSummary }}
    </button>
    <div v-if="turn.steps.length && (!finished || chainOpen)" class="steps">
      <template v-for="(block, bi) in blocks" :key="`${block.kind}-${bi}`">
        <div :class="block.kind === 'group' ? 'sub-group' : 'top-rows'" :data-testid="block.kind === 'group' ? 'subtopic-group' : undefined">
          <div v-if="block.kind === 'group'" class="sub-heading" data-testid="subtopic-heading">{{ block.title }}</div>
          <template v-for="item in groupSteps(block.rows)" :key="item.kind === 'row' ? item.row.id : item.id">
            <StepRow v-if="item.kind === 'row'" :row="item.row" :expanded="isExpanded(item.row)" @toggle="toggle(item.row)" @raw="openRaw" />
            <StepGroup v-else :item="item" :is-expanded="isExpanded" @toggle="toggle" @raw="openRaw" />
          </template>
        </div>
      </template>
    </div>
    <div v-if="writing" class="writing" data-testid="writing-report" role="status">
      ✍️ 正在整理资料并撰写报告… {{ writingSeconds }} 秒
    </div>

    <QuestionCard
      v-if="turn.question"
      :question="turn.question"
      :disabled="busy || turn.status !== 'awaiting_input'"
      @answer="(a) => emit('answer', turn.turnId, a)"
    />

    <!-- 有报告时 reply 只是摘要加指向报告的提示，报告全文已在下方，不再重复 -->
    <div v-if="turn.reply && !turn.report" class="reply" data-testid="reply">{{ turn.reply }}</div>

    <StopCard
      :stop="turn.stop"
      :status="turn.status"
      :can-restore="turn.canRestore"
      :busy="busy"
      @continue="emit('action', turn.turnId, 'continue')"
      @finish="emit('action', turn.turnId, 'finish')"
      @restore="emit('restore', turn.turnId)"
    />

    <template v-if="turn.report">
      <ReportBody :turn="turn" />
      <button class="report-card" type="button" data-testid="report-card" data-action="view-report" @click="emit('panel', 'report')">
        <span class="rc-icon" aria-hidden="true">📄</span>
        <span class="rc-main">
          <span class="rc-title">{{ turn.report.title }}</span>
          <span class="rc-meta">
            <span v-if="turn.report.partial" class="partial">部分</span>
            <span v-if="turn.report.toolBudgetReached" class="budget">已达工具额度</span>
            <span class="open">在右侧打开全文 →</span>
          </span>
        </span>
      </button>
      <p v-if="turn.report.note" class="r-note">{{ turn.report.note }}</p>
    </template>

    <div v-if="turn.error" class="t-error" role="alert">{{ turn.error }}</div>

    <RawDialog :turn-id="turn.turnId" :raw-ref="rawRef" @close="rawRef = null" />
  </div>
</template>

<script setup lang="ts">
// 一轮对话中 AI 一侧的渲染：路径标识、步骤行（按子主题分组）、流式回复（纯文本）、提问卡、停止卡、报告全文与报告卡片、失败提示。
// 进行中的非思考步骤默认展开、其余折叠（思考链默认不展开）；用户点击后以用户的选择为准。
// 连续的"思考"与连续的"阅读网页"各合并为一行；一轮结束后整条链折叠为"研究过程 · N 步 · 用时"一行。
// 子主题都结束、报告未到时显示"正在撰写报告"计时行（从最后一个事件起计）；报告到达后全文显示在对话中间。
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import type { Answer, TurnAction } from "../../api/chat";
import { routeLabel } from "../../lib/chat";
import type { RawRef, StepRow as StepRowData, TurnView } from "../../lib/chat";
import { durationText, groupSteps, isWritingReport, turnFinished } from "../../lib/steps";
import QuestionCard from "./QuestionCard.vue";
import RawDialog from "./RawDialog.vue";
import ReportBody from "./ReportBody.vue";
import StepGroup from "./StepGroup.vue";
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
  return overrides.value.get(row.id) ?? (row.status === "running" && row.kind !== "thinking");
}

function toggle(row: StepRowData): void {
  const next = new Map(overrides.value);
  next.set(row.id, !isExpanded(row));
  overrides.value = next;
}

function openRaw(r: RawRef): void {
  rawRef.value = r;
}

// 步骤行按子主题归组：并行的子主题（sub-run）事件交错到达，同一子主题的行仍放在一起，分组出现在该子主题
// 第一行的位置。分组键是 subtopicId（缺省时用子主题标题）；research_subtopic 行本身不归组。分组标题只显示
// 子主题名，不显示任何 ID。不属于子主题的相邻顶层行累积为一个 top 块，块内与分组内一样合并连续的思考/阅读网页行。
type Block = { kind: "top"; rows: StepRowData[] } | { kind: "group"; key: string; title: string; rows: StepRowData[] };

const blocks = computed<Block[]>(() => {
  const t = props.turn;
  const out: Block[] = [];
  const groups = new Map<string, Extract<Block, { kind: "group" }>>();
  for (const row of t.steps) {
    const key = row.kind === "subtopic" ? undefined : (row.subtopicId ?? row.subtopic);
    if (!key) {
      const last = out[out.length - 1];
      if (last?.kind === "top") last.rows.push(row);
      else out.push({ kind: "top", rows: [row] });
      continue;
    }
    let g = groups.get(key);
    if (!g) {
      g = { kind: "group", key, title: "", rows: [] };
      groups.set(key, g);
      out.push(g);
    }
    g.rows.push(row);
  }
  for (const g of groups.values()) {
    g.title =
      g.rows.find((r) => r.subtopic)?.subtopic ??
      t.subtopics.find((s) => s.id === g.key)?.title ??
      t.todo.find((x) => x.id === g.key)?.title ??
      "子主题";
  }
  return out;
});

const chainOpen = ref(false);
const finished = computed(() => turnFinished(props.turn));
const chainSummary = computed(() => {
  const t = props.turn;
  const n = t.steps.length;
  const ms = t.startedAt !== undefined && t.lastEventAt !== undefined ? t.lastEventAt - t.startedAt : 0;
  return ms > 0 ? `研究过程 · ${n} 步 · ${durationText(ms)}` : `研究过程 · ${n} 步`;
});

// 撰写报告期间模型调用不流式、没有事件：每秒刷新一次计时
const now = ref(Date.now());
let tick: ReturnType<typeof setInterval> | undefined;
onMounted(() => {
  tick = setInterval(() => (now.value = Date.now()), 1000);
});
onBeforeUnmount(() => {
  if (tick) clearInterval(tick);
});

const writing = computed(() => isWritingReport(props.turn));
const writingSeconds = computed(() => {
  const at = props.turn.lastEventAt;
  return at === undefined ? 0 : Math.max(0, Math.round((now.value - at) / 1000));
});

const label = computed(() => routeLabel(props.turn));
// 恢复出的轮次（没有 route 事件时）也是研究：可查看进度
const isResearch = computed(() => props.turn.route === "research" || (!props.turn.route && !!props.turn.restoredFrom));

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
.chain-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  border: 0;
  background: none;
  padding: 3px 6px;
  border-radius: 6px;
  color: #656d76;
  font: inherit;
  cursor: pointer;
}
.chain-toggle:hover {
  background: #f6f8fa;
}
.top-rows,
.sub-group {
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
.writing {
  margin: 6px 0;
  padding: 6px 10px;
  border-radius: 8px;
  background: #fff8f0;
  color: #9a6700;
  font-size: 13px;
}
.report-card {
  display: flex;
  gap: 10px;
  align-items: center;
  width: 100%;
  max-width: 520px;
  margin: 10px 0;
  padding: 12px 14px;
  border: 1px solid #d0d7de;
  border-radius: 10px;
  background: #fff;
  text-align: left;
  font: inherit;
  cursor: pointer;
}
.report-card:hover {
  border-color: #d97757;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.06);
}
.rc-icon {
  font-size: 22px;
}
.rc-main {
  display: grid;
  gap: 4px;
  min-width: 0;
}
.rc-title {
  font-weight: 600;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.rc-meta {
  display: flex;
  gap: 8px;
  font-size: 12px;
  color: #656d76;
}
.partial,
.budget {
  padding: 0 6px;
  border-radius: 10px;
  background: #fdf1ec;
  color: #b4532f;
}
.open {
  color: #0969da;
}
.r-note {
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
