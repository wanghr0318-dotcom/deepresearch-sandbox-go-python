<template>
  <div class="artifacts">
    <ErrorBanner :error="error" />
    <div v-if="rows.length === 0" class="empty">还没有产物（产物来自 artifact 事件与任务结果）</div>
    <table v-else class="data-table">
      <thead>
        <tr>
          <th>artifact_id</th>
          <th>声明类型</th>
          <th>版本</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.artifactId" :data-artifact="row.artifactId">
          <td>
            <div class="mono strong">{{ row.artifactId }}</div>
            <div v-if="row.path" class="mono muted small">{{ row.path }}</div>
          </td>
          <td>
            <span class="mono">{{ row.mediaType ?? "—" }}</span>
            <span v-if="row.visibility === 'internal'" class="badge internal">internal</span>
          </td>
          <td>
            <div v-if="row.versions.length === 0" class="muted small">未保存</div>
            <div v-for="v in row.versions" :key="v.version" class="version" :data-version="v.version">
              <span class="mono">v{{ v.version }}</span>
              <span v-if="v.pinned" class="badge pinned" title="结果固定的版本">pinned</span>
              <span v-if="v.sha256" class="mono muted small" :title="v.sha256">{{ v.sha256.slice(0, 12) }}</span>
              <template v-if="row.visibility !== 'internal'">
                <button
                  class="btn small"
                  type="button"
                  data-action="download"
                  :disabled="busyKey === key(row, v.version)"
                  @click="download(row, v.version)"
                >
                  下载
                </button>
                <button
                  v-if="mayPreview(row)"
                  class="btn small"
                  type="button"
                  data-action="preview"
                  :disabled="busyKey === key(row, v.version)"
                  @click="preview(row, v.version)"
                >
                  预览
                </button>
              </template>
              <span v-else class="muted small">internal 产物不可下载</span>
            </div>
          </td>
        </tr>
      </tbody>
    </table>

    <section v-if="shown" class="panel preview" :data-preview-kind="shown.kind">
      <header class="preview-header">
        <span class="mono strong">{{ shown.artifactId }} v{{ shown.version }}</span>
        <span class="mono muted small">{{ shown.contentType }}</span>
        <span class="spacer"></span>
        <button class="btn small" type="button" @click="saveShown">下载</button>
        <button class="btn small" type="button" @click="shown = null">关闭</button>
      </header>
      <!-- html 已经过 marked → DOMPurify 清理（lib/markdown.ts），不含脚本、事件属性、javascript: 链接与 iframe。 -->
      <!-- eslint-disable-next-line vue/no-v-html -->
      <div v-if="shown.kind === 'markdown'" class="md-preview" v-html="shown.html"></div>
      <pre v-else-if="shown.kind === 'text'" class="raw-json">{{ shown.text }}</pre>
      <div v-else class="notice" data-testid="download-only">
        该内容类型（{{ shown.contentType || "未知" }}）属于主动内容或无法安全预览，只提供下载，不在页面内显示。
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { TaskEvent, TaskResult } from "../api/client";
import { collectArtifacts } from "../lib/artifacts";
import type { ArtifactRow } from "../lib/artifacts";
import { renderMarkdown } from "../lib/markdown";
import { baseMediaType, downloadFilename, previewKindForDownload, previewKindOf } from "../lib/media";
import type { PreviewKind } from "../lib/media";
import { useServices } from "../lib/services";
import ErrorBanner from "./ErrorBanner.vue";

const props = defineProps<{ taskId: string; events: TaskEvent[]; result: TaskResult | null }>();

const { api, saveBlob } = useServices();
const rows = computed(() => collectArtifacts(props.events, props.result));
const busyKey = ref("");
const error = ref<unknown>(null);

interface Shown {
  artifactId: string;
  version: number;
  contentType: string;
  kind: PreviewKind;
  html: string;
  text: string;
  blob: Blob;
  filename: string;
}
const shown = ref<Shown | null>(null);

function key(row: ArtifactRow, version: number): string {
  return `${row.artifactId}@${version}`;
}

/** 声明为 HTML/SVG 等不可预览类型时只给下载按钮；类型未知时允许尝试预览（以响应头为准）。 */
function mayPreview(row: ArtifactRow): boolean {
  return !row.mediaType || previewKindOf(row.mediaType) !== "none";
}

async function fetchVersion(row: ArtifactRow, version: number) {
  busyKey.value = key(row, version);
  error.value = null;
  try {
    return await api.downloadArtifact(props.taskId, row.artifactId, version);
  } catch (e) {
    error.value = e;
    return null;
  } finally {
    busyKey.value = "";
  }
}

async function download(row: ArtifactRow, version: number): Promise<void> {
  const d = await fetchVersion(row, version);
  if (!d) return;
  saveBlob(d.blob, downloadFilename(d.contentDisposition, row.artifactId, version));
}

async function preview(row: ArtifactRow, version: number): Promise<void> {
  const d = await fetchVersion(row, version);
  if (!d) return;
  const kind = previewKindForDownload(d.contentType, d.contentDisposition);
  const text = kind === "none" ? "" : await d.blob.text();
  shown.value = {
    artifactId: row.artifactId,
    version,
    contentType: baseMediaType(d.contentType),
    kind,
    html: kind === "markdown" ? renderMarkdown(text) : "",
    text: kind === "text" ? text : "",
    blob: d.blob,
    filename: downloadFilename(d.contentDisposition, row.artifactId, version),
  };
}

function saveShown(): void {
  if (shown.value) saveBlob(shown.value.blob, shown.value.filename);
}
</script>

<style scoped>
.artifacts {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.strong {
  font-weight: 700;
}
.small {
  font-size: 0.72rem;
}
.version {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 2px 0;
}
.preview {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.preview-header {
  display: flex;
  align-items: center;
  gap: 10px;
}
.md-preview {
  line-height: 1.65;
  color: #1f2937;
  font-size: 0.92rem;
  overflow-wrap: anywhere;
}
.md-preview :deep(h1),
.md-preview :deep(h2),
.md-preview :deep(h3) {
  line-height: 1.3;
  margin: 1.1em 0 0.5em;
}
.md-preview :deep(pre) {
  background: #f3f4f6;
  padding: 10px 12px;
  border-radius: 8px;
  overflow: auto;
}
.md-preview :deep(code) {
  font-family: "JetBrains Mono", Consolas, monospace;
  font-size: 0.85em;
}
.md-preview :deep(a) {
  color: #6d28d9;
}
.md-preview :deep(table) {
  border-collapse: collapse;
}
.md-preview :deep(th),
.md-preview :deep(td) {
  border: 1px solid #e5e7eb;
  padding: 4px 8px;
}
.md-preview :deep(img) {
  max-width: 100%;
}
.md-preview :deep(blockquote) {
  margin: 0;
  padding-left: 12px;
  border-left: 3px solid #ddd6fe;
  color: #4b5563;
}
</style>
