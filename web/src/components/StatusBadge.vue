<template>
  <span class="badge" :class="cls" :title="title">{{ label || status || "unknown" }}</span>
</template>

<script setup lang="ts">
import { computed } from "vue";

// label：显示文字（用户页用中文状态名）；缺省显示原始状态值（工作台）。
const props = defineProps<{ status: string; title?: string; label?: string }>();

const KNOWN = new Set(["queued", "running", "pausing", "paused", "cancelling", "cancelled", "succeeded", "failed"]);

// 只把已知状态映射为 class，未知取值按默认样式显示。
const cls = computed(() => (KNOWN.has(props.status) ? props.status : ""));
</script>
