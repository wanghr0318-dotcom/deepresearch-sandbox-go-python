<template>
  <div class="shell">
    <!-- 左侧边栏（结构与样式沿用 helloagents 工作台） -->
    <nav class="sidebar">
      <div class="sidebar-logo">
        <svg viewBox="0 0 24 24" class="logo-icon" aria-hidden="true">
          <path
            d="M12 2.5c-.7 0-1.4.2-2 .6L4.6 7C3.6 7.6 3 8.7 3 9.9v4.2c0 1.2.6 2.3 1.6 2.9l5.4 3.9c1.2.8 2.8.8 4 0l5.4-3.9c1-.7 1.6-1.7 1.6-2.9V9.9c0-1.2-.6-2.3-1.6-2.9L14 3.1a3.6 3.6 0 0 0-2-.6Z"
          />
        </svg>
        <span class="logo-text">agentbox 工作台</span>
      </div>

      <div class="sidebar-section-label">导航</div>
      <div class="nav-links">
        <a class="nav-link" :class="{ active: route.name === 'tasks' }" :href="tasksHref()">任务列表</a>
        <a v-if="route.name === 'task'" class="nav-link active mono" :href="taskHref(route.id)" :title="route.id">
          {{ shortId(route.id, 18) }}
        </a>
      </div>

      <div class="sidebar-section-label">访问令牌</div>
      <div class="token-state">
        {{ authState.hasToken ? `已设置（${authState.persistence === "session" ? "sessionStorage" : "仅内存"}）` : "未设置" }}
      </div>
      <form class="token-form" autocomplete="off" @submit.prevent="saveToken">
        <input
          v-model="tokenInput"
          class="token-input"
          type="password"
          autocomplete="off"
          placeholder="Bearer token（本机访问可留空）"
          aria-label="API token"
        />
        <label class="token-persist">
          <input v-model="persistSession" type="checkbox" />
          本标签页内记住（sessionStorage）
        </label>
        <div class="token-actions">
          <button class="new-btn" type="submit">保存</button>
          <button class="ghost-btn" type="button" @click="clearToken">清除</button>
        </div>
      </form>

      <div class="sidebar-footer">
        <div class="conn-dot" :class="statusClass"></div>
        <span class="conn-text">{{ statusText }}</span>
        <button class="ghost-btn small" type="button" @click="refreshStatus">刷新</button>
      </div>
    </nav>

    <!-- 主内容区：hash 路由（#/admin/tasks、#/admin/tasks/<id>），不与 API 路径冲突 -->
    <main class="main">
      <div v-if="status && status.mode !== 'normal'" class="mode-banner" role="status">
        {{ MODE_TEXT[status.mode] ?? status.mode }}
      </div>
      <TokenGate v-if="!authState.hasToken && !authState.skipped" @done="refreshStatus" />
      <TaskDetailView v-else-if="route.name === 'task'" :id="route.id" :key="`${route.id}:${authEpoch}`" />
      <TaskListView v-else :key="`list:${authEpoch}`" />
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, provide, ref } from "vue";
import { ApiError } from "./api/client";
import type { Status } from "./api/client";
import TokenGate from "./components/TokenGate.vue";
import { authState, clearToken as clearStoredToken, setToken } from "./lib/authState";
import { shortId } from "./lib/format";
import { taskHref, tasksHref } from "./lib/router";
import type { AdminRoute } from "./lib/router";
import { defaultServices, servicesKey } from "./lib/services";
import TaskDetailView from "./views/TaskDetailView.vue";
import TaskListView from "./views/TaskListView.vue";

// 运维工作台（#/admin/...）：TokenGate、视图与行为与原工作台一致；只是路由前缀移到了 #/admin。
defineProps<{ route: AdminRoute }>();

const services = inject(servicesKey, null) ?? defaultServices();
provide(servicesKey, services);
const api = services.api;

const MODE_TEXT: Record<string, string> = {
  diagnostic: "服务处于诊断模式：只读，提交与控制会被拒绝（diagnostic_mode）。",
  ownership_lost: "服务已失去数据目录所有权：不再提交任何写入（ownership_lost）。",
};

// 令牌变化后重建视图，使请求与事件流使用新令牌。
const authEpoch = ref(0);
const tokenInput = ref("");
const persistSession = ref(authState.persistence === "session");
const status = ref<Status | null>(null);
const statusError = ref("");

const statusClass = computed(() => {
  if (statusError.value) return "err";
  if (!status.value) return "warn";
  return status.value.mode === "normal" ? "ok" : "warn";
});

const statusText = computed(() => {
  if (statusError.value) return statusError.value;
  if (!status.value) return "连接中...";
  return `模式：${status.value.mode}`;
});

async function refreshStatus(): Promise<void> {
  statusError.value = "";
  try {
    status.value = await api.getStatus();
  } catch (e) {
    status.value = null;
    statusError.value = e instanceof ApiError ? `${e.status} ${e.code}` : "无法连接服务";
  }
}

function saveToken(): void {
  setToken(tokenInput.value, persistSession.value ? "session" : "memory");
  tokenInput.value = "";
  authEpoch.value++;
  void refreshStatus();
}

function clearToken(): void {
  clearStoredToken();
  persistSession.value = false;
  authEpoch.value++;
  void refreshStatus();
}

onMounted(() => {
  void refreshStatus();
});
</script>

<style scoped>
.shell {
  display: flex;
  height: 100vh;
  overflow: hidden;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  background: #f9fafb;
  color: #111827;
}

.sidebar {
  width: 240px;
  flex-shrink: 0;
  background: #111827;
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  border-right: 1px solid #1f2937;
}

.sidebar-logo {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 20px 16px 12px;
  border-bottom: 1px solid #1f2937;
}

.logo-icon {
  width: 28px;
  height: 28px;
  fill: #7c3aed;
  flex-shrink: 0;
}

.logo-text {
  font-size: 0.95rem;
  font-weight: 700;
  color: #f9fafb;
  letter-spacing: -0.02em;
}

.sidebar-section-label {
  padding: 12px 16px 6px;
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #4b5563;
}

.token-form {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 0 12px;
  flex: 1;
}

.token-input {
  width: 100%;
  padding: 8px 10px;
  border-radius: 6px;
  border: 1px solid #374151;
  background: #1f2937;
  color: #f9fafb;
  font-size: 0.82rem;
}

.nav-links {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 0 8px 6px;
}

.nav-link {
  display: block;
  padding: 7px 10px;
  border-radius: 6px;
  color: #9ca3af;
  font-size: 0.85rem;
  text-decoration: none;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.nav-link:hover {
  background: #1f2937;
  color: #e5e7eb;
}
.nav-link.active {
  background: #1f2937;
  color: #f9fafb;
  font-weight: 600;
}
.nav-link.mono {
  font-family: "JetBrains Mono", Consolas, monospace;
  font-size: 0.76rem;
  padding-left: 20px;
}

.token-state {
  padding: 0 16px 8px;
  font-size: 0.75rem;
  color: #9ca3af;
}

.mode-banner {
  padding: 10px 32px;
  background: #fef3c7;
  color: #78350f;
  border-bottom: 1px solid #fde68a;
  font-size: 0.85rem;
  font-weight: 600;
}

.token-persist {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.75rem;
  color: #9ca3af;
}

.token-actions {
  display: flex;
  gap: 8px;
}

.new-btn {
  padding: 8px 14px;
  background: #7c3aed;
  color: white;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-size: 0.85rem;
  font-weight: 600;
  transition: background 0.15s;
}
.new-btn:hover {
  background: #6d28d9;
}

.ghost-btn {
  padding: 8px 14px;
  background: transparent;
  color: #9ca3af;
  border: 1px solid #374151;
  border-radius: 8px;
  cursor: pointer;
  font-size: 0.85rem;
}
.ghost-btn:hover {
  color: #d1d5db;
  background: #1f2937;
}
.ghost-btn.small {
  margin-left: auto;
  padding: 2px 8px;
  font-size: 0.72rem;
}

.sidebar-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 16px;
  border-top: 1px solid #1f2937;
}

.conn-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}
.conn-dot.ok {
  background: #10b981;
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.5);
}
.conn-dot.warn {
  background: #f59e0b;
}
.conn-dot.err {
  background: #ef4444;
}

.conn-text {
  font-size: 0.75rem;
  color: #6b7280;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.main {
  flex: 1;
  overflow-y: auto;
  background: #ffffff;
  display: flex;
  flex-direction: column;
}

.welcome {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 60px 24px 40px;
  max-width: 760px;
  margin: 0 auto;
  width: 100%;
  gap: 8px;
}

.welcome-title {
  font-size: 2rem;
  font-weight: 700;
  color: #111827;
  margin: 0;
  letter-spacing: -0.03em;
}

.welcome-sub {
  font-size: 1rem;
  color: #6b7280;
  margin: 0;
}
</style>
