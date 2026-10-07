// 报告加载：按 (turnId, artifactId, version) 缓存；active() 为真时加载；切换报告时丢弃旧结果。
// 右侧面板的"报告"标签与对话中间的报告全文共用。
import { computed, onBeforeUnmount, ref, watch } from "vue";
import type { TurnView } from "./chat";
import { renderMarkdown } from "./markdown";
import { useUserServices } from "./userServices";

export interface LoadedReport {
  key: string;
  blob: Blob;
  html: string;
}

export function useReport(turn: () => TurnView | undefined, active: () => boolean) {
  const { chat } = useUserServices();
  const report = ref<LoadedReport | null>(null);
  const loading = ref(false);
  const loadError = ref("");
  let generation = 0;

  const key = computed(() => {
    const t = turn();
    return t?.report ? `${t.turnId}\n${t.report.artifactId}\n${t.report.version}` : "";
  });

  async function load(): Promise<void> {
    const t = turn();
    const k = key.value;
    if (!t?.report || !k || report.value?.key === k) return;
    const gen = ++generation;
    report.value = null;
    loadError.value = "";
    loading.value = true;
    try {
      const dl = await chat.downloadArtifact(t.turnId, t.report.artifactId, t.report.version);
      const text = await dl.blob.text();
      if (gen !== generation) return;
      report.value = { key: k, blob: dl.blob, html: renderMarkdown(text) };
    } catch {
      if (gen === generation) loadError.value = "报告加载失败，请稍后重试";
    } finally {
      if (gen === generation) loading.value = false;
    }
  }

  watch(
    () => [active(), key.value] as const,
    ([on, k]) => {
      if (report.value && report.value.key !== k) {
        generation++;
        report.value = null;
        loading.value = false;
        loadError.value = "";
      }
      if (on) void load();
    },
    { immediate: true },
  );
  onBeforeUnmount(() => {
    generation++;
  });

  return { report, loading, loadError };
}
