<template>
  <div v-if="view" class="error-banner" role="alert" :data-code="view.code">
    <span class="error-title">{{ view.title }}</span>
    <span class="error-code mono">{{ view.status ? `${view.status} ` : "" }}{{ view.code }}</span>
    <div v-if="view.detail" class="error-detail">{{ view.detail }}</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { describeError } from "../lib/errors";

const props = defineProps<{ error: unknown }>();

const view = computed(() => (props.error == null ? null : describeError(props.error)));
</script>
