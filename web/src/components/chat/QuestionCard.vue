<template>
  <div class="ask" :class="{ answered: question.answered }">
    <div class="a-head">❓ 需要确认 {{ question.questions.length }} 个问题</div>

    <fieldset v-for="(q, qi) in question.questions" :key="qi" class="q" data-testid="question" :disabled="locked">
      <legend class="q-text">{{ qi + 1 }}. {{ q.question }}</legend>
      <div class="opts">
        <button
          v-for="(o, oi) in q.options"
          :key="oi"
          class="opt"
          :class="{ sel: picks[qi]?.choice === o }"
          type="button"
          data-action="option"
          :aria-pressed="picks[qi]?.choice === o ? 'true' : 'false'"
          :disabled="locked"
          @click="choose(qi, o)"
        >
          {{ o }}
        </button>
        <button
          v-if="q.allowOther"
          class="opt"
          :class="{ sel: picks[qi]?.other }"
          type="button"
          data-action="other"
          :aria-pressed="picks[qi]?.other ? 'true' : 'false'"
          :disabled="locked"
          @click="chooseOther(qi)"
        >
          其他…
        </button>
      </div>
      <input
        v-if="q.allowOther && picks[qi]?.other && !question.answered"
        v-model="picks[qi]!.text"
        class="other-input"
        type="text"
        :maxlength="OTHER_MAX"
        :aria-label="`${q.question}（其他）`"
        placeholder="请输入你的回答（1–200 字）"
        :disabled="locked"
      />
    </fieldset>

    <div v-if="question.answered" class="done">
      ✓ 已回答<template v-if="question.answers?.length">：{{ question.answers.join("；") }}</template>
    </div>
    <div v-else class="a-foot">
      <button class="btn primary small" type="button" data-action="submit-answers" :disabled="!ready" @click="submit">提交</button>
      <span v-if="!complete" class="hint">请回答每一个问题</span>
    </div>
  </div>
</template>

<script setup lang="ts">
// 提问卡（ask_user）：每题单选选项，可选"其他…"自由输入（1–200 字）；全部作答后才能提交；
// 轮次带着回答继续后（question.answered）显示"已回答"与本页记下的回答内容。
import { computed, ref, watch } from "vue";
import type { Answer } from "../../api/chat";
import type { Question } from "../../lib/chat";

const OTHER_MAX = 200;

interface Pick {
  choice?: string;
  other: boolean;
  text: string;
}

const props = defineProps<{ question: Question; disabled: boolean }>();
const emit = defineEmits<{ answer: [answers: Answer[]] }>();

const picks = ref<Pick[]>([]);

watch(
  () => props.question.questionId,
  () => {
    picks.value = props.question.questions.map(() => ({ other: false, text: "" }));
  },
  { immediate: true },
);

const locked = computed(() => props.disabled || props.question.answered);

function choose(i: number, option: string): void {
  const p = picks.value[i];
  if (!p || locked.value) return;
  p.choice = option;
  p.other = false;
}

function chooseOther(i: number): void {
  const p = picks.value[i];
  if (!p || locked.value) return;
  p.choice = undefined;
  p.other = true;
}

function otherText(p: Pick): string {
  return p.text.trim();
}

function validOther(p: Pick): boolean {
  const n = [...otherText(p)].length;
  return n >= 1 && n <= OTHER_MAX;
}

const complete = computed(() =>
  props.question.questions.length > 0 &&
  props.question.questions.every((_, i) => {
    const p = picks.value[i];
    return !!p && (p.choice !== undefined || (p.other && validOther(p)));
  }),
);
const ready = computed(() => complete.value && !locked.value);

function submit(): void {
  if (!ready.value) return;
  const answers: Answer[] = props.question.questions.map((q, i) => {
    const p = picks.value[i]!;
    return p.other ? { question_id: q.id, other: otherText(p) } : { question_id: q.id, choice: p.choice! };
  });
  emit("answer", answers);
}
</script>

<style scoped>
.ask {
  border: 1px solid #d0d7de;
  border-radius: 10px;
  padding: 10px 12px;
  background: #fbfcfd;
  margin-left: 24px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  font-size: 0.86rem;
}
.a-head {
  font-weight: 700;
  color: #1f2328;
}
.q {
  border: 0;
  margin: 0;
  padding: 0;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.q-text {
  padding: 0;
  font-weight: 600;
  color: #1f2328;
}
.opts {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.opt {
  border: 1px solid #d0d7de;
  border-radius: 14px;
  padding: 3px 10px;
  background: #ffffff;
  color: #424a53;
  font: inherit;
  font-size: 0.8rem;
  cursor: pointer;
}
.opt:hover:not(:disabled) {
  border-color: #a78bfa;
}
.opt.sel {
  border-color: #7c3aed;
  background: #f5f3ff;
  color: #5b21b6;
  font-weight: 600;
}
.opt:disabled {
  cursor: default;
}
.opt:disabled:not(.sel) {
  opacity: 0.6;
}
.opt:focus-visible,
.other-input:focus-visible {
  outline: 2px solid #7c3aed;
  outline-offset: 1px;
}
.other-input {
  max-width: 420px;
  border: 1px solid #d0d7de;
  border-radius: 8px;
  padding: 5px 9px;
  font: inherit;
  font-size: 0.84rem;
}
.a-foot {
  display: flex;
  align-items: center;
  gap: 10px;
}
.hint {
  font-size: 0.76rem;
  color: #8c959f;
}
.done {
  font-size: 0.8rem;
  color: #1a7f37;
  font-weight: 600;
  word-break: break-word;
}
</style>
