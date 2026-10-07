<template>
  <div class="auth-page">
    <div class="auth-card">
      <div class="auth-brand">
        <BrandMark />
        <span>DeepResearch 助手</span>
      </div>
      <h1 class="auth-title">创建账号</h1>
      <p class="auth-sub">输入一个主题，助手会规划子任务、检索资料，并写成带引用的研究报告。</p>

      <form class="auth-form" novalidate @submit.prevent="submit">
        <label class="field">
          用户名
          <input v-model="username" name="username" autocomplete="username" maxlength="32" placeholder="3–32 位字母、数字、_ . -" />
        </label>
        <label class="field">
          密码
          <input v-model="password" name="password" type="password" autocomplete="new-password" maxlength="16" placeholder="8–16 位，数字/大写/小写至少两种" />
        </label>
        <ul class="pw-checks" data-testid="pw-checks" aria-live="polite">
          <li :class="{ ok: checks.length }" data-check="length">{{ checks.length ? "✓" : "✗" }} 8–16 个字符</li>
          <li :class="{ ok: checks.classes }" data-check="classes">{{ checks.classes ? "✓" : "✗" }} 数字、大写字母、小写字母至少两种</li>
        </ul>
        <label class="field">
          确认密码
          <input v-model="confirm" name="confirm" type="password" autocomplete="new-password" maxlength="16" placeholder="再输入一次密码" />
        </label>
        <div v-if="error" class="auth-error" role="alert">{{ error }}</div>
        <button class="btn primary auth-submit" type="submit" :disabled="busy || !checks.length || !checks.classes">{{ busy ? "注册中…" : "注册并登录" }}</button>
      </form>

      <p class="auth-switch">已有账号？<a class="link" :href="loginHref()">去登录</a></p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { User } from "../api/client";
import { validateRegistration } from "../api/session";
import BrandMark from "../components/BrandMark.vue";
import { passwordChecks } from "../lib/password";
import { userErrorMessage } from "../lib/research";
import { loginHref } from "../lib/router";
import { useUserServices } from "../lib/userServices";

const emit = defineEmits<{ "signed-in": [user: User] }>();

const { api } = useUserServices();
const username = ref("");
const password = ref("");
const confirm = ref("");
const error = ref("");
const busy = ref(false);
const checks = computed(() => passwordChecks(password.value));

async function submit(): Promise<void> {
  if (busy.value) return;
  error.value = validateRegistration(username.value, password.value, confirm.value);
  if (error.value) return;
  busy.value = true;
  try {
    const user = await api.register(username.value, password.value);
    password.value = "";
    confirm.value = "";
    emit("signed-in", user);
  } catch (e) {
    error.value = userErrorMessage(e);
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.pw-checks { list-style: none; margin: -4px 0 4px; padding: 0; font-size: 12px; color: #8c959f; display: grid; gap: 2px; }
.pw-checks li.ok { color: #1a7f37; }
</style>
