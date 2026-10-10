<template>
  <div class="auth-page">
    <div class="auth-card">
      <div class="auth-brand">
        <BrandMark />
        <span>DeepResearch 助手</span>
      </div>
      <h1 class="auth-title">欢迎回来</h1>
      <p class="auth-sub">登录后即可开始新的研究，查看你的研究报告。</p>

      <form class="auth-form" novalidate @submit.prevent="submit">
        <label class="field">
          用户名
          <input v-model="username" name="username" autocomplete="username" maxlength="32" placeholder="你的用户名" />
        </label>
        <label class="field">
          密码
          <input v-model="password" name="password" type="password" autocomplete="current-password" maxlength="128" placeholder="至少 8 个字符" />
        </label>
        <div v-if="error" class="auth-error" role="alert">{{ error }}</div>
        <button class="btn primary auth-submit" type="submit" :disabled="busy">{{ busy ? "登录中…" : "登录" }}</button>
      </form>

      <p class="auth-switch">还没有账号？<a class="link" :href="registerHref()">注册一个</a></p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import type { User } from "../api/client";
import { validateLogin } from "../api/session";
import BrandMark from "../components/BrandMark.vue";
import { userErrorMessage } from "../lib/userErrors";
import { registerHref } from "../lib/router";
import { useUserServices } from "../lib/userServices";

const emit = defineEmits<{ "signed-in": [user: User] }>();

const { api } = useUserServices();
const username = ref("");
const password = ref("");
const error = ref("");
const busy = ref(false);

async function submit(): Promise<void> {
  if (busy.value) return;
  error.value = validateLogin(username.value, password.value);
  if (error.value) return;
  busy.value = true;
  try {
    const user = await api.login(username.value, password.value);
    password.value = "";
    emit("signed-in", user);
  } catch (e) {
    error.value = userErrorMessage(e);
  } finally {
    busy.value = false;
  }
}
</script>
