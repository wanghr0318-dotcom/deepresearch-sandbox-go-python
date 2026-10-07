<template>
  <div class="merged" :data-kind="item.rowKind" data-testid="merged-row">
    <button class="m-head" type="button" :aria-expanded="open ? 'true' : 'false'" @click="open = !open">
      <span class="k" aria-hidden="true">{{ open ? "▾" : "▸" }}</span>
      <span class="ico" aria-hidden="true">{{ item.rowKind === "thinking" ? "💭" : "🌐" }}</span>
      <span class="title">{{ item.title }}</span>
      <span v-if="running" class="dot" aria-label="进行中"></span>
    </button>
    <div v-if="open" class="m-rows">
      <StepRow v-for="row in item.rows" :key="row.id" :row="row" :expanded="isExpanded(row)" @toggle="emit('toggle', row)" @raw="(r) => emit('raw', r)" />
    </div>
  </div>
</template>

<script setup lang="ts">
// 合并行：默认折叠（思考链默认不展开）；点击展开为各条原始行，每行仍可单独展开并保留 ⟨/⟩。
import { computed, ref } from "vue";
import type { RawRef, StepRow as StepRowData } from "../../lib/chat";
import type { StepItem } from "../../lib/steps";
import StepRow from "./StepRow.vue";

const props = defineProps<{ item: Extract<StepItem, { kind: "merged" }>; isExpanded: (row: StepRowData) => boolean }>();
const emit = defineEmits<{ toggle: [row: StepRowData]; raw: [r: RawRef] }>();
const open = ref(false);
const running = computed(() => props.item.rows.some((r) => r.status === "running"));
</script>

<style scoped>
.m-head { display: flex; align-items: center; gap: 6px; width: 100%; padding: 3px 6px; border: 0; background: none; border-radius: 6px; color: #424a53; font: inherit; text-align: left; cursor: pointer; }
.m-head:hover { background: #f6f8fa; }
.k { color: #8c959f; width: 10px; }
.dot { width: 6px; height: 6px; border-radius: 50%; background: #d97757; }
.m-rows { margin-left: 16px; border-left: 2px solid #eaeef2; padding-left: 6px; }
</style>
