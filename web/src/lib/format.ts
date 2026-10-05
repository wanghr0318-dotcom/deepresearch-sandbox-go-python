// 展示用的格式化函数。

/** 微美元 → "$0.001234"。 */
export function formatMicroUSD(micro: number | undefined | null): string {
  const v = Number(micro ?? 0);
  if (!Number.isFinite(v)) return "—";
  return `$${(v / 1_000_000).toFixed(6)}`;
}

export function formatMs(ms: number | undefined | null): string {
  const v = Number(ms ?? 0);
  if (!Number.isFinite(v)) return "—";
  return v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${Math.round(v)} ms`;
}

/** 本地时间 HH:MM:SS.mmm。 */
export function formatTime(ts: string | undefined | null): string {
  if (!ts) return "—";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

export function shortId(id: string | undefined | null, n = 14): string {
  if (!id) return "—";
  return id.length > n ? `${id.slice(0, n)}…` : id;
}
