// 对话页三栏宽度：会话栏与右侧面板可拖动，对话栏至少 chatMin；宽度记在本浏览器（失败时用默认值）。
export const LAYOUT = {
  sidebar: { min: 200, max: 420, def: 264 },
  panel: { min: 320, max: 720, def: 380 },
  chatMin: 420,
} as const;

const KEY = "agentbox.chat.widths";
type Widths = { sidebar: number; panel: number };

const clamp = (v: number, lo: number, hi: number): number => Math.min(hi, Math.max(lo, v));

export function clampWidths(w: Widths, total: number, panelOpen: boolean): Widths {
  let sidebar = clamp(w.sidebar, LAYOUT.sidebar.min, LAYOUT.sidebar.max);
  let panel = clamp(w.panel, LAYOUT.panel.min, LAYOUT.panel.max);
  if (!panelOpen) return { sidebar: Math.min(sidebar, Math.max(LAYOUT.sidebar.min, total - LAYOUT.chatMin)), panel };
  const over = sidebar + panel + LAYOUT.chatMin - total;
  if (over > 0) {
    const fromPanel = Math.min(over, panel - LAYOUT.panel.min);
    panel -= fromPanel;
    sidebar = Math.max(LAYOUT.sidebar.min, sidebar - (over - fromPanel));
  }
  return { sidebar, panel };
}

export function loadWidths(): Widths {
  const def = { sidebar: LAYOUT.sidebar.def, panel: LAYOUT.panel.def };
  try {
    // 只存栏宽（非敏感），不涉及 token；§15.3 的禁令针对凭据。
    // eslint-disable-next-line no-restricted-globals
    const v = JSON.parse(localStorage.getItem(KEY) ?? "null") as Partial<Widths> | null;
    if (v && typeof v.sidebar === "number" && typeof v.panel === "number") return { sidebar: v.sidebar, panel: v.panel };
  } catch {
    /* 存储不可用或内容损坏：用默认值 */
  }
  return def;
}

export function saveWidths(w: Widths): void {
  try {
    // eslint-disable-next-line no-restricted-globals
    localStorage.setItem(KEY, JSON.stringify(w));
  } catch {
    /* 忽略：只是便利功能 */
  }
}
