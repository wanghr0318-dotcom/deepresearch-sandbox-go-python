<template>
  <form class="composer" :class="{ focused }" @submit.prevent="submit">
    <textarea
      ref="input"
      v-model="text"
      name="message"
      rows="1"
      aria-label="输入消息"
      :placeholder="placeholder ?? '问点什么…'"
      @keydown.enter="onEnter"
      @compositionstart="composing = true"
      @compositionend="composing = false"
      @focus="focused = true"
      @blur="focused = false"
      @input="error = ''"
    ></textarea>
    <div class="bar">
      <button
        class="toggle"
        :class="{ on: deepResearch }"
        type="button"
        data-action="deep-research"
        :aria-pressed="deepResearch ? 'true' : 'false'"
        title="强制检索网页并写成带引用的研究报告"
        @click="deepResearch = !deepResearch"
      >
        <span class="dot" aria-hidden="true"></span>深度研究
      </button>
      <span v-if="error" class="err" role="alert">{{ error }}</span>
      <span v-else-if="running" class="hint" data-testid="composer-hint">先停止当前研究</span>
      <span class="spacer"></span>
      <button
        v-if="running"
        class="round stop"
        type="button"
        data-action="stop-turn"
        aria-label="停止当前研究"
        title="停止当前研究"
        :disabled="stopping"
        @click="emit('stop')"
      >
        <span class="sq" aria-hidden="true"></span>
      </button>
      <button class="round send" type="submit" data-action="send" aria-label="发送" title="发送（Enter）" :disabled="!canSend">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 19V5M5.5 11.5 12 5l6.5 6.5" /></svg>
      </button>
    </div>
  </form>
</template>

<script setup lang="ts">
// 输入框：多行（随内容增高）；Enter 发送、Shift+Enter 换行、输入法组合中不发送；"深度研究"开关（aria-pressed）。
// 有进行中的轮次时禁用发送并提示"先停止当前研究"（契约裁定 I：运行中再发消息会 409 turn_in_progress），同时给出 ■ 停止。
// 发送成功后由父组件调用 clear()；失败时保留输入。
import { computed, nextTick, ref, watch } from "vue";
import { validateMessage } from "../../api/chat";

defineOptions({ name: "ChatComposer" });

const props = withDefaults(defineProps<{ disabled: boolean; placeholder?: string; running?: boolean; stopping?: boolean }>(), {
  placeholder: undefined,
  running: false,
  stopping: false,
});
const emit = defineEmits<{ send: [payload: { text: string; deepResearch: boolean }]; stop: [] }>();

const text = ref("");
const deepResearch = ref(false);
const composing = ref(false);
const focused = ref(false);
const error = ref("");
const input = ref<HTMLTextAreaElement | null>(null);

const canSend = computed(() => !props.disabled && !props.running && text.value.trim() !== "");

function onEnter(e: KeyboardEvent): void {
  if (e.shiftKey || e.isComposing || composing.value || e.keyCode === 229) return;
  e.preventDefault();
  submit();
}

function submit(): void {
  if (props.disabled || props.running) return;
  const msg = validateMessage(text.value);
  if (msg) {
    if (text.value.trim()) error.value = msg;
    return;
  }
  error.value = "";
  emit("send", { text: text.value.trim(), deepResearch: deepResearch.value });
}

function resize(): void {
  const el = input.value;
  if (!el) return;
  el.style.height = "auto";
  el.style.height = `${Math.min(el.scrollHeight, 220)}px`;
}

watch(text, () => void nextTick(resize));

function clear(): void {
  text.value = "";
  error.value = "";
}

function setText(v: string): void {
  text.value = v;
  focus();
}

function focus(): void {
  input.value?.focus();
}

defineExpose({ clear, setText, focus });
</script>

<style scoped>
.composer {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 12px 8px 14px;
  border: 1px solid #d9dce3;
  border-radius: 16px;
  background: #fff;
  box-shadow: 0 1px 2px rgba(17, 24, 39, 0.04), 0 4px 16px rgba(17, 24, 39, 0.05);
  transition: border-color 0.15s, box-shadow 0.15s;
}
.composer.focused {
  border-color: #a78bfa;
  box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.12), 0 4px 16px rgba(17, 24, 39, 0.06);
}
textarea {
  width: 100%;
  min-height: 26px;
  max-height: 220px;
  resize: none;
  border: 0;
  outline: none;
  padding: 2px 0;
  font: inherit;
  font-size: 0.95rem;
  line-height: 1.6;
  color: #111827;
  background: transparent;
}
textarea::placeholder {
  color: #9ca3af;
}
.bar {
  display: flex;
  align-items: center;
  gap: 10px;
  min-height: 32px;
}
.spacer {
  flex: 1;
}
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 11px;
  border-radius: 999px;
  border: 1px solid #e5e7eb;
  background: #f9fafb;
  color: #4b5563;
  font: inherit;
  font-size: 0.8rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.15s;
}
.toggle:hover {
  border-color: #c4b5fd;
}
.toggle .dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  border: 1.5px solid #9ca3af;
  transition: all 0.15s;
}
.toggle.on {
  border-color: #c4b5fd;
  background: #f5f3ff;
  color: #6d28d9;
}
.toggle.on .dot {
  border-color: #7c3aed;
  background: #7c3aed;
  box-shadow: 0 0 0 2px #ddd6fe;
}
.hint {
  font-size: 0.78rem;
  color: #92400e;
}
.err {
  font-size: 0.78rem;
  color: #b91c1c;
}
.round {
  flex: none;
  width: 34px;
  height: 34px;
  border-radius: 50%;
  border: 0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  transition: background 0.15s, opacity 0.15s;
}
.send {
  background: #7c3aed;
  color: #fff;
}
.send:hover:not(:disabled) {
  background: #6d28d9;
}
.send:disabled {
  background: #e5e7eb;
  color: #9ca3af;
  cursor: not-allowed;
}
.send svg {
  width: 18px;
  height: 18px;
  fill: none;
  stroke: currentColor;
  stroke-width: 2.4;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.stop {
  background: #111827;
}
.stop:hover:not(:disabled) {
  background: #374151;
}
.stop:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}
.stop .sq {
  width: 11px;
  height: 11px;
  border-radius: 2px;
  background: #fff;
}
</style>
