<template>
  <div
    class="splitter"
    role="separator"
    aria-orientation="vertical"
    :aria-label="label"
    :aria-valuemin="min"
    :aria-valuemax="max"
    :aria-valuenow="value"
    tabindex="0"
    :data-testid="`splitter-${label}`"
    @pointerdown="start"
    @keydown.left.prevent="nudge(-16)"
    @keydown.right.prevent="nudge(16)"
  ></div>
</template>

<script setup lang="ts">
// 竖向分隔线：拖动或用 ←/→（每次 16 px）调整相邻栏宽。invert 为真时向左拖动增大（右侧面板）。
defineOptions({ name: "PaneSplitter" });
const props = withDefaults(defineProps<{ value: number; min: number; max: number; label: string; invert?: boolean }>(), { invert: false });
const emit = defineEmits<{ "update:value": [v: number] }>();

const clamp = (v: number): number => Math.min(props.max, Math.max(props.min, Math.round(v)));

function nudge(dx: number): void {
  emit("update:value", clamp(props.value + (props.invert ? -dx : dx)));
}

function start(e: PointerEvent): void {
  const el = e.currentTarget as HTMLElement;
  el.setPointerCapture?.(e.pointerId);
  const x0 = e.clientX;
  const v0 = props.value;
  const move = (ev: PointerEvent): void => {
    const dx = ev.clientX - x0;
    emit("update:value", clamp(v0 + (props.invert ? -dx : dx)));
  };
  const up = (): void => {
    el.removeEventListener("pointermove", move);
    el.removeEventListener("pointerup", up);
    el.removeEventListener("pointercancel", up);
  };
  el.addEventListener("pointermove", move);
  el.addEventListener("pointerup", up);
  el.addEventListener("pointercancel", up);
}
</script>

<style scoped>
.splitter { width: 6px; margin: 0 -3px; cursor: col-resize; position: relative; z-index: 2; background: transparent; touch-action: none; }
.splitter::after { content: ""; position: absolute; inset: 0 2px; background: #d0d7de; opacity: 0; transition: opacity .15s; }
.splitter:hover::after, .splitter:focus-visible::after { opacity: 1; }
.splitter:focus-visible { outline: none; }
.splitter:focus-visible::after { background: #0969da; }
</style>
