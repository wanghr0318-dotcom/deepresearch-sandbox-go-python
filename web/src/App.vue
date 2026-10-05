<template>
  <!-- 运维工作台：#/admin/...（TokenGate 与全部视图不变） -->
  <AdminApp v-if="isAdminRoute(route)" :route="route" />

  <!-- 用户页：DeepResearch 助手（cookie 会话） -->
  <div v-else class="user-shell">
    <header v-if="user" class="topbar">
      <a class="topbar-brand" :href="homeHref()">
        <BrandMark />
        <span>DeepResearch 助手</span>
      </a>
      <span class="spacer"></span>
      <span class="topbar-user" data-testid="username">{{ user.username }}</span>
      <button class="btn small" type="button" data-action="logout" :disabled="signingOut" @click="logout">退出登录</button>
    </header>

    <main class="user-main">
      <div v-if="!checked" class="empty">加载中…</div>
      <template v-else-if="route.name === 'login' || route.name === 'register'">
        <div v-if="connError" class="conn-error user-error" role="alert">{{ connError }}</div>
        <LoginView v-if="route.name === 'login'" @signed-in="signedIn" />
        <RegisterView v-else @signed-in="signedIn" />
      </template>
      <template v-else-if="user">
        <ResearchDetailView v-if="route.name === 'research'" :id="route.id" :key="route.id" @unauthorized="signedOut" />
        <AssistantView v-else @unauthorized="signedOut" />
      </template>
    </main>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from "vue";
import AdminApp from "./AdminApp.vue";
import type { User } from "./api/client";
import BrandMark from "./components/BrandMark.vue";
import { userErrorMessage } from "./lib/research";
import { homeHref, isAdminRoute, loginHref, navigate, needsSession, useHashRoute } from "./lib/router";
import { useUserServices } from "./lib/userServices";
import AssistantView from "./views/AssistantView.vue";
import LoginView from "./views/LoginView.vue";
import RegisterView from "./views/RegisterView.vue";
import ResearchDetailView from "./views/ResearchDetailView.vue";

const route = useHashRoute();
const { api } = useUserServices();

// 当前登录用户：只来自 GET /auth/me 与登录/注册的响应；会话 cookie 是 HttpOnly 的，脚本从不接触。
const user = ref<User | null>(null);
const checked = ref(false);
const connError = ref("");
const signingOut = ref(false);

/** 守卫：未登录访问用户页 → #/login；已登录访问登录/注册页 → #/。 */
function guard(): void {
  if (!checked.value || isAdminRoute(route.value)) return;
  if (!user.value && needsSession(route.value)) navigate(loginHref());
  else if (user.value && (route.value.name === "login" || route.value.name === "register")) navigate(homeHref());
}

async function checkSession(): Promise<void> {
  try {
    user.value = await api.me();
    connError.value = "";
  } catch (e) {
    user.value = null;
    connError.value = userErrorMessage(e);
  } finally {
    checked.value = true;
    guard();
  }
}

function signedIn(u: User): void {
  user.value = u;
  connError.value = "";
  navigate(homeHref());
}

function signedOut(): void {
  user.value = null;
  navigate(loginHref());
}

async function logout(): Promise<void> {
  signingOut.value = true;
  try {
    await api.logout();
  } catch {
    // 网络失败时本地仍退出；服务端会话到期自然失效。
  } finally {
    signingOut.value = false;
    signedOut();
  }
}

watch(route, guard);

onMounted(() => {
  // 工作台不需要用户会话；只有用户页才查询登录状态。
  if (isAdminRoute(route.value)) {
    checked.value = true;
    return;
  }
  void checkSession();
});

// 从工作台切回用户页时补查一次登录状态。
watch(
  () => isAdminRoute(route.value),
  (admin, wasAdmin) => {
    if (!admin && wasAdmin && !user.value) {
      checked.value = false;
      void checkSession();
    }
  },
);
</script>

<style scoped>
.user-shell {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  color: #111827;
  background:
    radial-gradient(1200px 420px at 50% -120px, rgba(124, 58, 237, 0.09), transparent 70%),
    #f9fafb;
}
.topbar {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 24px;
  background: rgba(255, 255, 255, 0.85);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid #e5e7eb;
}
.topbar-brand {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 700;
  font-size: 0.95rem;
  color: #111827;
  text-decoration: none;
  letter-spacing: -0.01em;
}
.topbar-user {
  font-size: 0.85rem;
  color: #4b5563;
  font-weight: 600;
}
.user-main {
  flex: 1;
  display: flex;
  flex-direction: column;
}
.conn-error {
  max-width: 400px;
  width: calc(100% - 40px);
  margin: 32px auto -24px;
}
</style>
