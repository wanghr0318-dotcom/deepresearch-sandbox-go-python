<template>
  <div class="page">
    <header class="page-header">
      <h1 class="page-title">任务</h1>
      <span class="page-sub">第 {{ pageNo }} 页</span>
      <span class="spacer"></span>
      <button class="btn" type="button" data-action="refresh" :disabled="loading" @click="reload">刷新</button>
      <button class="btn primary" type="button" data-action="new" @click="showForm = !showForm">
        {{ showForm ? "收起" : "新建任务" }}
      </button>
    </header>

    <SubmitTaskForm v-if="showForm" @created="onCreated" />

    <ErrorBanner :error="error" />

    <div class="panel table-panel">
      <div v-if="loading && tasks.length === 0" class="empty">加载中…</div>
      <div v-else-if="tasks.length === 0" class="empty">还没有任务</div>
      <table v-else class="data-table">
        <thead>
          <tr>
            <th>task_id</th>
            <th>状态</th>
            <th>desired</th>
            <th>原因</th>
            <th class="num">attempts</th>
            <th class="num">control v</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="t in tasks" :key="t.task_id" :data-task="t.task_id">
            <td>
              <a class="link mono" :href="taskHref(t.task_id)">{{ t.task_id }}</a>
            </td>
            <td><StatusBadge :status="t.status" /></td>
            <td class="mono">{{ t.desired }}</td>
            <td class="muted small">{{ t.status_reason ?? "" }}</td>
            <td class="num">{{ t.attempts_total }}</td>
            <td class="num mono">{{ t.applied_control_version }}/{{ t.control_version }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <nav class="pager">
      <button class="btn" type="button" data-action="first" :disabled="loading || cursors.length === 0" @click="first">
        第一页
      </button>
      <button class="btn" type="button" data-action="prev" :disabled="loading || cursors.length === 0" @click="prev">
        上一页
      </button>
      <button class="btn" type="button" data-action="next" :disabled="loading || !next" @click="nextPage">下一页</button>
    </nav>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import type { Task } from "../api/client";
import ErrorBanner from "../components/ErrorBanner.vue";
import StatusBadge from "../components/StatusBadge.vue";
import SubmitTaskForm from "../components/SubmitTaskForm.vue";
import { navigate, taskHref } from "../lib/router";
import { useServices } from "../lib/services";

const props = withDefaults(defineProps<{ pageSize?: number }>(), { pageSize: 20 });

const { api } = useServices();

const tasks = ref<Task[]>([]);
const next = ref<string | undefined>(undefined);
// 游标栈：cursors[i] 是第 i+2 页的 after；当前页的游标是栈顶（第一页没有游标）。
const cursors = ref<string[]>([]);
const loading = ref(false);
const error = ref<unknown>(null);
const showForm = ref(false);
let seq = 0;

const pageNo = computed(() => cursors.value.length + 1);

async function load(): Promise<void> {
  const my = ++seq;
  loading.value = true;
  error.value = null;
  try {
    const after = cursors.value[cursors.value.length - 1];
    const page = await api.listTasks(after ? { after, limit: props.pageSize } : { limit: props.pageSize });
    if (my !== seq) return; // 较早的请求晚到：丢弃
    tasks.value = page.tasks;
    next.value = page.next || undefined;
  } catch (e) {
    if (my === seq) error.value = e;
  } finally {
    if (my === seq) loading.value = false;
  }
}

function reload(): void {
  void load();
}

function nextPage(): void {
  if (!next.value) return;
  cursors.value = [...cursors.value, next.value];
  void load();
}

function prev(): void {
  cursors.value = cursors.value.slice(0, -1);
  void load();
}

function first(): void {
  cursors.value = [];
  void load();
}

function onCreated(taskId: string): void {
  showForm.value = false;
  navigate(taskHref(taskId));
}

onMounted(() => {
  void load();
});
</script>

<style scoped>
.table-panel {
  padding: 6px 8px;
}
.pager {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}
.small {
  font-size: 0.78rem;
}
</style>
