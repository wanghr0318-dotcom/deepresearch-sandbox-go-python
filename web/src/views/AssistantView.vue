<template>
  <div class="assistant">
    <section class="hero">
      <h1 class="hero-title">想研究点什么？</h1>
      <p class="hero-sub">输入一个主题，助手会先拟定研究计划，再逐个子任务检索资料，最后写成带引用的研究报告。</p>

      <form class="ask" @submit.prevent="submit">
        <textarea
          v-model="topic"
          class="ask-input"
          name="topic"
          rows="3"
          :maxlength="TOPIC_MAX"
          placeholder="例如：固态电池的商业化进展与主要技术路线"
          aria-label="研究主题"
          @keydown.enter.exact.prevent="submit"
        ></textarea>
        <div class="ask-bar">
          <span class="muted count">{{ topicLength }} / {{ TOPIC_MAX }}</span>
          <span class="spacer"></span>
          <button class="btn primary ask-btn" type="submit" :disabled="busy">
            {{ busy ? "提交中…" : "开始研究" }}
          </button>
        </div>
      </form>
      <div class="examples">
        <span class="muted">试试：</span>
        <button v-for="ex in EXAMPLES" :key="ex" class="chip" type="button" @click="topic = ex">{{ ex }}</button>
      </div>
      <div v-if="submitError" class="user-error" role="alert" data-testid="submit-error">{{ submitError }}</div>
    </section>

    <section class="mine">
      <div class="mine-head">
        <h2 class="mine-title">我的研究</h2>
        <span class="spacer"></span>
        <button class="btn small" type="button" :disabled="loading" @click="loadList">刷新</button>
      </div>
      <div v-if="active" class="notice running-note">
        有一项研究正在进行中，
        <a class="link" :href="researchHref(active.task_id)">查看进度</a>
      </div>
      <div v-if="listError" class="user-error" role="alert">{{ listError }}</div>
      <div v-if="loading && tasks.length === 0" class="empty">加载中…</div>
      <div v-else-if="tasks.length === 0 && !listError" class="empty">还没有研究，从上面输入一个主题开始吧。</div>
      <ul v-else class="research-list">
        <li v-for="t in tasks" :key="t.task_id" class="research-item" :data-task="t.task_id">
          <a class="research-link" :href="researchHref(t.task_id)">
            <span class="research-text">
              <span class="research-topic">{{ researchTitle(t) }}</span>
              <span v-if="t.created_at" class="research-date" data-testid="created">{{ formatCreated(t.created_at) }}</span>
            </span>
            <StatusBadge :status="t.status" :label="statusLabel(t.status)" />
          </a>
        </li>
      </ul>
      <div v-if="next" class="more">
        <button class="btn" type="button" :disabled="loading" @click="loadMore">加载更多</button>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { ApiError, NetworkError, newRequestId } from "../api/client";
import type { Task } from "../api/client";
import { TOPIC_MAX, runeLength, validateTopic } from "../api/session";
import StatusBadge from "../components/StatusBadge.vue";
import { formatCreated, isActive, isUnauthorized, researchTitle, statusLabel, userErrorMessage } from "../lib/research";
import { navigate, researchHref } from "../lib/router";
import { useUserServices } from "../lib/userServices";

const props = withDefaults(defineProps<{ refreshMs?: number }>(), { refreshMs: 5000 });
const emit = defineEmits<{ unauthorized: [] }>();

const EXAMPLES = ["大语言模型智能体的沙箱隔离方案对比", "钠离子电池的产业化现状", "2026 年开源向量数据库选型"];

const { api } = useUserServices();

const topic = ref("");
const busy = ref(false);
const submitError = ref("");
const tasks = ref<Task[]>([]);
const next = ref<string | undefined>();
const loading = ref(false);
const listError = ref("");

// 失败结果未知（网络错误、可重试错误）时，同一主题的再次提交复用 request_id，避免重复创建。
let pendingRequest: { topic: string; id: string } | null = null;
let timer: ReturnType<typeof setInterval> | undefined;

const topicLength = computed(() => runeLength(topic.value.trim()));
const active = computed(() => tasks.value.find(isActive) ?? null);

function handle(e: unknown): string {
  if (isUnauthorized(e)) emit("unauthorized");
  return userErrorMessage(e);
}

async function loadList(): Promise<void> {
  loading.value = true;
  try {
    const page = await api.listTasks({ limit: 20 });
    tasks.value = page.tasks;
    next.value = page.next;
    listError.value = "";
  } catch (e) {
    listError.value = handle(e);
  } finally {
    loading.value = false;
  }
}

async function loadMore(): Promise<void> {
  if (!next.value) return;
  loading.value = true;
  try {
    const page = await api.listTasks({ after: next.value, limit: 20 });
    const seen = new Set(tasks.value.map((t) => t.task_id));
    tasks.value = [...tasks.value, ...page.tasks.filter((t) => !seen.has(t.task_id))];
    next.value = page.next;
  } catch (e) {
    listError.value = handle(e);
  } finally {
    loading.value = false;
  }
}

async function submit(): Promise<void> {
  if (busy.value) return;
  submitError.value = validateTopic(topic.value);
  if (submitError.value) return;
  const t = topic.value.trim();
  const requestId = pendingRequest?.topic === t ? pendingRequest.id : newRequestId();
  pendingRequest = { topic: t, id: requestId };
  busy.value = true;
  try {
    const res = await api.createResearch(t, requestId);
    pendingRequest = null;
    topic.value = "";
    navigate(researchHref(res.task_id));
  } catch (e) {
    if (!(e instanceof NetworkError || (e instanceof ApiError && e.retryable))) pendingRequest = null;
    submitError.value = handle(e);
  } finally {
    busy.value = false;
  }
}

onMounted(() => {
  void loadList();
  // 有研究进行中时定期刷新状态，演示时无需手动刷新。
  timer = setInterval(() => {
    if (active.value && !loading.value) void loadList();
  }, props.refreshMs);
});

onBeforeUnmount(() => clearInterval(timer));
</script>

<style scoped>
.assistant {
  width: 100%;
  max-width: 860px;
  margin: 0 auto;
  padding: 44px 24px 56px;
  display: flex;
  flex-direction: column;
  gap: 34px;
}
.hero {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.hero-title {
  margin: 0;
  font-size: 2rem;
  font-weight: 700;
  letter-spacing: -0.03em;
  color: #111827;
}
.hero-sub {
  margin: 0 0 6px;
  color: #6b7280;
  font-size: 0.98rem;
}
.ask {
  border: 1px solid #e5e7eb;
  border-radius: 16px;
  background: #ffffff;
  box-shadow: 0 6px 24px rgba(17, 24, 39, 0.06);
  padding: 14px 16px 12px;
  transition: border-color 0.15s, box-shadow 0.15s;
}
.ask:focus-within {
  border-color: #a78bfa;
  box-shadow: 0 6px 28px rgba(124, 58, 237, 0.14);
}
.ask-input {
  width: 100%;
  border: none;
  outline: none;
  resize: none;
  font: inherit;
  font-size: 1.02rem;
  line-height: 1.6;
  color: #111827;
  background: transparent;
}
.ask-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  padding-top: 6px;
}
.count {
  font-size: 0.75rem;
}
.ask-btn {
  padding: 9px 20px;
  border-radius: 10px;
}
.examples {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  font-size: 0.82rem;
}
.chip {
  border: 1px solid #e5e7eb;
  background: #ffffff;
  color: #4b5563;
  border-radius: 999px;
  padding: 4px 12px;
  font-size: 0.8rem;
  cursor: pointer;
}
.chip:hover {
  border-color: #c4b5fd;
  color: #6d28d9;
  background: #f5f3ff;
}
.mine {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.mine-head {
  display: flex;
  align-items: center;
}
.mine-title {
  margin: 0;
  font-size: 1.1rem;
  font-weight: 700;
  color: #111827;
}
.research-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.research-link {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px 16px;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  background: #ffffff;
  text-decoration: none;
  color: #111827;
  transition: border-color 0.15s, box-shadow 0.15s;
}
.research-link:hover {
  border-color: #c4b5fd;
  box-shadow: 0 4px 14px rgba(124, 58, 237, 0.08);
}
.research-text {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.research-date {
  font-size: 0.75rem;
  color: #9ca3af;
}
.research-topic {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-weight: 600;
  font-size: 0.95rem;
}
.more {
  display: flex;
  justify-content: center;
}
</style>
