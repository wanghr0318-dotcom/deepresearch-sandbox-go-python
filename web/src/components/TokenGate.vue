<template>
  <div class="gate">
    <div class="panel gate-card">
      <h1 class="page-title">连接 agentbox</h1>
      <p class="muted">
        输入 API 访问令牌。令牌默认只保存在本页内存中，只放在请求的 Authorization 头里，从不出现在 URL。
      </p>
      <form class="gate-form" autocomplete="off" @submit.prevent="submit">
        <label class="field">
          访问令牌
          <input v-model="token" type="password" autocomplete="off" placeholder="Bearer token" aria-label="访问令牌" />
        </label>
        <label class="persist">
          <input v-model="persist" type="checkbox" />
          在本标签页内记住（sessionStorage，关闭标签页即失效）
        </label>
        <div class="actions">
          <button class="btn primary" type="submit" :disabled="!token.trim()">连接</button>
          <button class="btn" type="button" @click="skip">不使用令牌（本机访问）</button>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { setToken, skipToken } from "../lib/authState";

const emit = defineEmits<{ done: [] }>();

const token = ref("");
const persist = ref(false);

function submit(): void {
  if (!token.value.trim()) return;
  setToken(token.value, persist.value ? "session" : "memory");
  token.value = "";
  emit("done");
}

function skip(): void {
  skipToken();
  emit("done");
}
</script>

<style scoped>
.gate {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 40px 24px;
}
.gate-card {
  width: 100%;
  max-width: 460px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 26px 28px;
}
.gate-form {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.persist {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.8rem;
  color: #4b5563;
}
.actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
</style>
