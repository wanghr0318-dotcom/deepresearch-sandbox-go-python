<template>
  <span class="money" :title="usdExact(micro)">{{ usd(micro) }}</span>
</template>

<script setup lang="ts">
// 金额：显示两位小数（与表格、合计一致），悬停显示到微美元的精确值。
import { usd, usdExact } from "../lib/gateway";

defineProps<{ micro: number | null | undefined }>();
</script>

<style scoped>
.money {
  font-variant-numeric: tabular-nums;
}
</style>
