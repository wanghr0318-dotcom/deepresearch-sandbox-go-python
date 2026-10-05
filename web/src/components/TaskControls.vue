<template>
  <div class="controls">
    <div class="buttons">
      <button class="btn" type="button" data-action="pause" :disabled="!avail.pause || busy !== null" @click="run('pause')">
        {{ busy === "pause" ? "暂停中…" : "暂停" }}
      </button>
      <button class="btn" type="button" data-action="resume" :disabled="!avail.resume || busy !== null" @click="run('resume')">
        {{ busy === "resume" ? "恢复中…" : "恢复" }}
      </button>
      <button class="btn danger" type="button" data-action="cancel" :disabled="!avail.cancel || busy !== null" @click="run('cancel')">
        {{ busy === "cancel" ? "取消中…" : "取消" }}
      </button>
    </div>
    <ErrorBanner :error="error" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { ControlResult, Task } from "../api/client";
import { ApiError, NetworkError, newRequestId } from "../api/client";
import { controlAvailability } from "../lib/controls";
import { useServices } from "../lib/services";
import ErrorBanner from "./ErrorBanner.vue";

type Action = "cancel" | "pause" | "resume";

const props = defineProps<{ task: Task | null }>();
const emit = defineEmits<{ changed: [result: ControlResult] }>();

const { api } = useServices();
const busy = ref<Action | null>(null);
const error = ref<unknown>(null);
// 每个动作失败后保留 request_id，再次点击时复用（服务端幂等）；成功后丢弃。
const pendingIds = new Map<string, string>();

// 只按服务端返回的任务事实启用按钮。
const avail = computed(() => controlAvailability(props.task));

async function run(action: Action): Promise<void> {
  const task = props.task;
  if (!task || busy.value) return;
  const key = `${task.task_id}:${action}`;
  const requestId = pendingIds.get(key) ?? newRequestId();
  pendingIds.set(key, requestId);
  busy.value = action;
  error.value = null;
  try {
    const call = action === "cancel" ? api.cancelTask : action === "pause" ? api.pauseTask : api.resumeTask;
    const res = await call.call(api, task.task_id, undefined, requestId);
    pendingIds.delete(key);
    emit("changed", res);
  } catch (e) {
    // 只有结果未知的失败（网络错误、可重试的 5xx/429）才保留 request_id 供重试；明确被拒的控制换新 id。
    if (!(e instanceof NetworkError || (e instanceof ApiError && e.retryable))) pendingIds.delete(key);
    error.value = e;
  } finally {
    busy.value = null;
  }
}
</script>

<style scoped>
.controls {
  display: flex;
  flex-direction: column;
  gap: 8px;
  align-items: flex-end;
}
.buttons {
  display: flex;
  gap: 8px;
}
</style>
