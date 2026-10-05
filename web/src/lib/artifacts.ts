// 由事件与结果汇总产物及其版本（API 没有单独的产物列表端点）：
// - Worker 的 artifact 事件声明 media_type、visibility、path（声明值，仅用于展示与预判）；
// - 宿主的 artifact_saved 事件给出已保存的 (artifact_id, version, sha256)；
// - 结果（/result）把输出固定为 (artifact_id, version, sha256)，标记为 pinned。
// 下载时以服务端响应的 Content-Type / Content-Disposition 为准（见 media.ts）。
import type { TaskEvent, TaskResult } from "../api/client";

export interface ArtifactVersion {
  version: number;
  sha256?: string;
  pinned: boolean;
}

export interface ArtifactRow {
  artifactId: string;
  /** Worker 声明的媒体类型（可能缺失）。 */
  mediaType?: string;
  /** Worker 声明的可见性；internal 产物不可下载。 */
  visibility?: string;
  path?: string;
  /** 版本号降序。 */
  versions: ArtifactVersion[];
}

function str(v: unknown): string | undefined {
  return typeof v === "string" && v !== "" ? v : undefined;
}

export function collectArtifacts(events: readonly TaskEvent[], result: TaskResult | null | undefined): ArtifactRow[] {
  const rows = new Map<string, ArtifactRow>();
  const row = (id: string): ArtifactRow => {
    let r = rows.get(id);
    if (!r) {
      r = { artifactId: id, versions: [] };
      rows.set(id, r);
    }
    return r;
  };
  const addVersion = (r: ArtifactRow, version: number, sha256: string | undefined, pinned: boolean) => {
    const existing = r.versions.find((v) => v.version === version);
    if (existing) {
      existing.pinned ||= pinned;
      existing.sha256 ??= sha256;
    } else {
      r.versions.push({ version, sha256, pinned });
    }
  };

  for (const ev of events) {
    const p = ev.payload ?? {};
    const id = str(p["artifact_id"]);
    if (!id) continue;
    if (ev.source === "worker" && ev.type === "artifact") {
      const r = row(id);
      r.mediaType = str(p["media_type"]) ?? r.mediaType;
      r.visibility = str(p["visibility"]) ?? r.visibility;
      r.path = str(p["path"]) ?? r.path;
    } else if (ev.source === "host" && ev.type === "artifact_saved") {
      const v = p["version"];
      if (typeof v === "number" && Number.isSafeInteger(v) && v > 0) addVersion(row(id), v, str(p["sha256"]), false);
    }
  }
  for (const out of result?.outputs ?? []) {
    const r = row(out.artifact_id);
    if (typeof out.version === "number" && out.version > 0) addVersion(r, out.version, out.sha256, true);
  }
  for (const r of rows.values()) r.versions.sort((a, b) => b.version - a.version);
  return [...rows.values()].sort((a, b) => a.artifactId.localeCompare(b.artifactId));
}
