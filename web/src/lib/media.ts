// 产物的展示策略（规格 §15.4、§5.6；契约 Blob 响应）：
// - 只有 text/markdown（经 marked → DOMPurify）以及 text/plain、application/json（纯文本插值）可以内联预览；
// - text/html、image/svg+xml 以及其他一切类型只提供下载，从不内联；
// - 服务端给出 Content-Disposition: attachment 的内容同样只下载。

export type PreviewKind = "markdown" | "text" | "none";

/** 去掉参数并小写，例如 "Text/Markdown; charset=utf-8" → "text/markdown"。 */
export function baseMediaType(contentType: string | undefined | null): string {
  return (contentType ?? "").split(";")[0]!.trim().toLowerCase();
}

/** 由媒体类型决定能否内联预览；未知类型返回 "none"。 */
export function previewKindOf(contentType: string | undefined | null): PreviewKind {
  switch (baseMediaType(contentType)) {
    case "text/markdown":
    case "text/x-markdown":
      return "markdown";
    case "text/plain":
    case "application/json":
      return "text";
    default:
      return "none";
  }
}

/** 下载响应的预览判定：服务端要求 attachment 时一律不内联。 */
export function previewKindForDownload(contentType: string, contentDisposition: string): PreviewKind {
  if (/^\s*attachment/i.test(contentDisposition)) return "none";
  return previewKindOf(contentType);
}

/** 从 Content-Disposition 取文件名；没有时用 artifact_id 与版本拼一个，并去掉路径分隔符。 */
export function downloadFilename(contentDisposition: string, artifactId: string, version: number): string {
  let name = "";
  const star = /filename\*\s*=\s*utf-8''([^;]+)/i.exec(contentDisposition);
  if (star?.[1]) {
    try {
      name = decodeURIComponent(star[1].trim());
    } catch {
      name = "";
    }
  }
  if (!name) {
    const plain = /filename\s*=\s*"?([^";]+)"?/i.exec(contentDisposition);
    if (plain?.[1]) name = plain[1].trim();
  }
  if (!name) name = `${artifactId}-v${version}`;
  return name.replace(/[\\/\0]/g, "_");
}
