// 报告加载：按 (turnId, artifactId, version) 缓存；active() 为真时加载；切换报告时丢弃旧结果。
// 右侧面板的"报告"标签与对话中间的报告全文共用：同一份报告只下载一次（缓存挂在对话服务对象上，
// 进行中的下载也共享；失败不缓存，下次重试）。
import { computed, onBeforeUnmount, ref, watch } from "vue";
import type { TurnView } from "./chat";
import { renderMarkdown } from "./markdown";
import { useUserServices } from "./userServices";

export interface LoadedReport {
  key: string;
  blob: Blob;
  html: string;
}

type ChatService = ReturnType<typeof useUserServices>["chat"];

/** 每个对话服务对象最多缓存的报告数（按最近使用淘汰）。 */
const CACHE_LIMIT = 8;
const caches = new WeakMap<ChatService, Map<string, Promise<LoadedReport>>>();

function loadShared(chat: ChatService, key: string, turnId: string, artifactId: string, version: number): Promise<LoadedReport> {
  let cache = caches.get(chat);
  if (!cache) {
    cache = new Map();
    caches.set(chat, cache);
  }
  const hit = cache.get(key);
  if (hit) {
    cache.delete(key); // 最近使用：移到末尾
    cache.set(key, hit);
    return hit;
  }
  const p = (async () => {
    const dl = await chat.downloadArtifact(turnId, artifactId, version);
    const text = await dl.blob.text();
    return { key, blob: dl.blob, html: renderMarkdown(text) };
  })();
  cache.set(key, p);
  p.catch(() => {
    if (cache.get(key) === p) cache.delete(key);
  });
  while (cache.size > CACHE_LIMIT) cache.delete(cache.keys().next().value!);
  return p;
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
      const loaded = await loadShared(chat, k, t.turnId, t.report.artifactId, t.report.version);
      if (gen !== generation) return;
      report.value = loaded;
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
