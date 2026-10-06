<template>
  <div v-if="role === 'user'" class="msg user" data-role="user"><slot /></div>
  <div v-else class="msg assistant" data-role="assistant">
    <span class="avatar" aria-hidden="true">AI</span>
    <div class="body"><slot /></div>
  </div>
</template>

<script setup lang="ts">
// 一条消息：用户在右侧（浅紫气泡，纯文本 pre-wrap）；AI 在左侧（头像 + 内容列，内容由默认插槽提供）。
defineProps<{ role: "user" | "assistant" }>();
</script>

<style scoped>
.msg.user {
  align-self: flex-end;
  max-width: min(78%, 640px);
  padding: 10px 14px;
  border-radius: 18px 18px 4px 18px;
  background: #ede9fe;
  color: #1f1147;
  font-size: 0.94rem;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
}
.msg.assistant {
  align-self: stretch;
  display: flex;
  gap: 12px;
  min-width: 0;
}
.avatar {
  flex: none;
  width: 28px;
  height: 28px;
  margin-top: 1px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.02em;
  color: #fff;
  background: linear-gradient(135deg, #8b5cf6, #6d28d9);
  box-shadow: 0 1px 2px rgba(109, 40, 217, 0.3);
}
.body {
  flex: 1;
  min-width: 0;
}
</style>
