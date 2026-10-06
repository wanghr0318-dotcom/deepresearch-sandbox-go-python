<template>
  <div v-if="rawRef" class="backdrop" @click.self="emit('close')">
    <div
      ref="dialog"
      class="dialog"
      role="dialog"
      aria-modal="true"
      aria-labelledby="raw-dialog-title"
      tabindex="-1"
      @keydown.esc.stop="emit('close')"
    >
      <header class="d-head">
        <h2 id="raw-dialog-title" class="d-title">原始请求与响应</h2>
        <button class="btn small" type="button" data-action="close" @click="emit('close')">关闭</button>
      </header>

      <section class="part">
        <h3 class="p-title">
          请求
          <span v-if="rawRef.requestTruncated" class="trunc">内容较长，已截断</span>
        </h3>
        <pre class="body">{{ pretty(rawRef.request) }}</pre>
      </section>

      <section class="part">
        <h3 class="p-title">响应</h3>
        <p v-if="loading" class="muted">加载中…</p>
        <p v-else-if="error" class="fail" role="alert">{{ error }}</p>
        <pre v-else class="body">{{ pretty(response) }}</pre>
      </section>
    </div>
  </div>
</template>

<script setup lang="ts">
// ⟨/⟩ 原始请求与响应：请求内联在事件里（服务端已脱敏，可能被截断）；响应经 GET /turns/{id}/raw/{sha} 读取（服务端已脱敏）。
// 一律以文本显示（<pre> 插值，不使用 v-html）；不显示响应的 sha 或任何内部 ID。
import { nextTick, onBeforeUnmount, ref, watch } from "vue";
import type { RawRef } from "../../lib/chat";
import { useUserServices } from "../../lib/userServices";

const props = defineProps<{ turnId: string; rawRef: RawRef | null }>();
const emit = defineEmits<{ close: [] }>();

const { chat } = useUserServices();

const dialog = ref<HTMLElement | null>(null);
const loading = ref(false);
const error = ref("");
const response = ref<unknown>(undefined);
let generation = 0;

function pretty(v: unknown): string {
  if (v === undefined || v === null) return "（无内容）";
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

async function load(): Promise<void> {
  const gen = ++generation;
  response.value = undefined;
  error.value = "";
  const r = props.rawRef;
  if (!r) {
    loading.value = false;
    return;
  }
  loading.value = true;
  try {
    const content = await chat.getRaw(props.turnId, r.responseRef);
    if (gen !== generation) return;
    response.value = content.body;
  } catch {
    if (gen === generation) error.value = "原始响应加载失败，请稍后重试";
  } finally {
    if (gen === generation) loading.value = false;
  }
}

watch(
  () => [props.turnId, props.rawRef] as const,
  () => {
    void load();
    void nextTick(() => dialog.value?.focus());
  },
  { immediate: true },
);

onBeforeUnmount(() => {
  generation++;
});
</script>

<style scoped>
.backdrop {
  position: fixed;
  inset: 0;
  z-index: 50;
  background: rgba(17, 24, 39, 0.38);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
.dialog {
  width: min(760px, 100%);
  max-height: 86vh;
  overflow: auto;
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 18px 48px rgba(17, 24, 39, 0.22);
  padding: 18px 20px 20px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.dialog:focus {
  outline: none;
}
.d-head {
  display: flex;
  align-items: center;
  gap: 10px;
}
.d-title {
  flex: 1;
  margin: 0;
  font-size: 1rem;
  font-weight: 700;
}
.part {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.p-title {
  margin: 0;
  font-size: 0.82rem;
  font-weight: 700;
  color: #4b5563;
  display: flex;
  align-items: center;
  gap: 8px;
}
.trunc {
  font-weight: 600;
  font-size: 0.72rem;
  color: #92400e;
  background: #fef3c7;
  border-radius: 8px;
  padding: 0 6px;
}
.body {
  margin: 0;
  background: #f6f8fa;
  border: 1px solid #eaeef2;
  border-radius: 8px;
  padding: 10px 12px;
  font-family: "JetBrains Mono", Consolas, monospace;
  font-size: 0.78rem;
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 34vh;
  overflow: auto;
}
.fail {
  margin: 0;
  color: #b91c1c;
  font-size: 0.85rem;
}
</style>
