<template>
  <span class="badge" :class="cls" :title="title">{{ status || "unknown" }}</span>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{ status: string; title?: string }>();

const KNOWN = new Set(["queued", "running", "pausing", "paused", "cancelling", "cancelled", "succeeded", "failed"]);

// 只把已知状态映射为 class，未知取值按默认样式显示。
const cls = computed(() => (KNOWN.has(props.status) ? props.status : ""));
</script>
