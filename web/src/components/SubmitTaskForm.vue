<template>
  <form class="panel submit-form" autocomplete="off" @submit.prevent="submit">
    <h2 class="panel-title">提交任务</h2>
    <div class="grid">
      <label class="field spec">
        spec（JSON，原样传给 Worker）
        <textarea v-model="specText" spellcheck="false" aria-label="spec JSON"></textarea>
        <span v-if="specError" class="hint err">{{ specError }}</span>
      </label>
      <fieldset class="limits">
        <legend>limits（留空 = 使用服务端默认）</legend>
        <label v-for="f in LIMIT_FIELDS" :key="f.key" class="field">
          <span class="mono">{{ f.key }}</span>
          <input
            v-model="limitInputs[f.key]"
            inputmode="numeric"
            :name="f.key"
            :placeholder="f.placeholder"
            :aria-label="f.key"
          />
          <span class="hint">{{ f.hint }}</span>
        </label>
      </fieldset>
    </div>
    <span v-if="limitsError" class="hint err">{{ limitsError }}</span>
    <ErrorBanner :error="error" />
    <div class="actions">
      <button class="btn primary" type="submit" :disabled="busy">
        {{ busy ? "提交中…" : retrying ? "重试提交（同一 request_id）" : "提交" }}
      </button>
      <span v-if="retrying" class="muted small">request_id <span class="mono">{{ pending?.requestId }}</span></span>
    </div>
  </form>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from "vue";
import { newRequestId } from "../api/client";
import { useServices } from "../lib/services";
import ErrorBanner from "./ErrorBanner.vue";

const emit = defineEmits<{ created: [taskId: string] }>();

const LIMIT_FIELDS = [
  { key: "max_run_time_ms", placeholder: "600000", hint: "累计运行时限（毫秒）" },
  { key: "budget_micro", placeholder: "500000", hint: "模型调用预算（微美元，1e6 = $1）" },
  { key: "memory_max", placeholder: "536870912", hint: "内存上限（字节）" },
  { key: "cpu_quota_us", placeholder: "100000", hint: "每 100 ms 周期的 CPU 配额（微秒）" },
  { key: "pids_max", placeholder: "256", hint: "进程数上限" },
  { key: "nofile", placeholder: "1024", hint: "打开文件数上限" },
  { key: "tmp_bytes", placeholder: "67108864", hint: "/tmp 容量（字节）" },
] as const;

type LimitKey = (typeof LIMIT_FIELDS)[number]["key"];

const { api } = useServices();

const specText = ref(JSON.stringify({ summary: "hello from the workbench", steps: [] }, null, 2));
const limitInputs = reactive<Record<LimitKey, string>>(
  Object.fromEntries(LIMIT_FIELDS.map((f) => [f.key, ""])) as Record<LimitKey, string>,
);
const busy = ref(false);
const error = ref<unknown>(null);
const specError = ref("");
const limitsError = ref("");

// 失败后保留 request_id：内容不变时再次提交复用它，服务端按幂等语义返回首次结果；
// 内容变了则换新的 request_id（同一 request_id 配不同内容会被拒为 request_conflict）。
const pending = ref<{ requestId: string; fingerprint: string } | null>(null);

function parseInput(): { spec: Record<string, unknown>; limits?: Record<string, number> } | null {
  specError.value = "";
  limitsError.value = "";
  let spec: unknown;
  try {
    spec = JSON.parse(specText.value);
  } catch (e) {
    specError.value = `spec 不是合法 JSON：${e instanceof Error ? e.message : String(e)}`;
    return null;
  }
  if (typeof spec !== "object" || spec === null || Array.isArray(spec)) {
    specError.value = "spec 必须是 JSON 对象";
    return null;
  }
  const limits: Record<string, number> = {};
  for (const f of LIMIT_FIELDS) {
    const raw = limitInputs[f.key].trim();
    if (!raw) continue;
    if (!/^\d+$/.test(raw) || !Number.isSafeInteger(Number(raw))) {
      limitsError.value = `${f.key} 必须是非负整数`;
      return null;
    }
    limits[f.key] = Number(raw);
  }
  return Object.keys(limits).length ? { spec: spec as Record<string, unknown>, limits } : { spec: spec as Record<string, unknown> };
}

const fingerprintNow = computed(() => {
  const parsed = (() => {
    try {
      return JSON.stringify({ s: JSON.parse(specText.value), l: { ...limitInputs } });
    } catch {
      return "";
    }
  })();
  return parsed;
});

const retrying = computed(() => pending.value !== null && pending.value.fingerprint === fingerprintNow.value);

async function submit(): Promise<void> {
  if (busy.value) return;
  const input = parseInput();
  if (!input) return;
  const fingerprint = fingerprintNow.value;
  if (!pending.value || pending.value.fingerprint !== fingerprint) {
    pending.value = { requestId: newRequestId(), fingerprint };
  }
  busy.value = true;
  error.value = null;
  try {
    const res = await api.createTask(input, pending.value.requestId);
    pending.value = null;
    emit("created", res.task_id);
  } catch (e) {
    error.value = e;
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.submit-form {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.grid {
  display: grid;
  grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr);
  gap: 16px;
}
@media (max-width: 860px) {
  .grid {
    grid-template-columns: 1fr;
  }
}
.spec textarea {
  min-height: 260px;
}
.limits {
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 10px 12px 12px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin: 0;
}
.limits legend {
  font-size: 0.75rem;
  font-weight: 700;
  color: #6b7280;
  padding: 0 4px;
}
.actions {
  display: flex;
  align-items: center;
  gap: 10px;
}
.small {
  font-size: 0.75rem;
}
.err {
  color: #b91c1c;
}
</style>
