<template>
  <div class="chat-view" :class="{ narrow }" @keydown.esc="drawer = null">
    <SessionSidebar
      v-if="!narrow"
      :sessions="sessions"
      :active-id="currentId"
      :loading="sessionsLoading"
      @create="newChat"
      @select="selectSession"
      @rename="renameSession"
      @remove="removeSession"
    />

    <section class="conv" aria-label="对话">
      <header v-if="narrow || currentId" class="conv-head">
        <button v-if="narrow" class="head-btn" type="button" data-action="open-sessions" @click="drawer = 'sessions'">
          <span aria-hidden="true">☰</span> 会话
        </button>
        <h1 class="conv-title" :title="title">{{ title }}</h1>
        <button v-if="narrow" class="head-btn" type="button" data-action="open-panel" @click="drawer = 'panel'">进度/来源/报告</button>
      </header>

      <div v-if="restoring" class="banner info" role="status" data-testid="restoring">
        <span class="spinner" aria-hidden="true"></span>正在恢复对话…
      </div>
      <div v-else-if="state.sessionMessage" class="banner warn" role="status" data-testid="session-message">{{ state.sessionMessage }}</div>

      <div ref="scroller" class="scroll" @scroll="onScroll">
        <div class="msgs">
          <div v-if="notFound" class="empty-state">
            <h2 class="hero-title">{{ notFound }}</h2>
            <p class="hero-sub">从左侧选一个对话，或者开始一个新对话。</p>
            <button class="btn primary" type="button" @click="newChat">开始新对话</button>
          </div>

          <div v-else-if="!currentId && !state.turns.length" class="empty-state">
            <div class="hero-mark" aria-hidden="true"><BrandMark /></div>
            <h2 class="hero-title">有什么想了解的？</h2>
            <p class="hero-sub">日常问题会直接回答；需要查最新资料时，打开「深度研究」，助手会检索网页、整理来源，并写成带引用的报告。</p>
            <div class="examples">
              <button v-for="ex in EXAMPLES" :key="ex" class="chip" type="button" @click="composer?.setText(ex)">{{ ex }}</button>
            </div>
          </div>

          <div v-else-if="turnsLoading && !state.turns.length" class="loading-note">加载中…</div>

          <template v-for="t in state.turns" :key="t.turnId">
            <MessageBubble v-if="t.userText" role="user">{{ t.userText }}</MessageBubble>
            <MessageBubble role="assistant">
              <TurnView
                :turn="t"
                :busy="busyTurns.has(t.turnId)"
                @answer="answer"
                @action="control"
                @restore="restore"
                @panel="(tab) => openPanel(t.turnId, tab)"
              />
            </MessageBubble>
          </template>
        </div>
      </div>

      <div class="dock">
        <div v-if="error" class="chat-error" role="alert">
          <span data-testid="chat-error">{{ error }}</span>
          <button class="x" type="button" aria-label="关闭提示" @click="error = ''">×</button>
        </div>
        <Composer
          ref="composer"
          :disabled="sending || !!notFound"
          :running="!!runningTurn"
          :stopping="!runningTurn || runningTurn.status === 'stopping' || busyTurns.has(runningTurn.turnId)"
          :placeholder="state.turns.length ? '继续提问…' : '问点什么，或打开「深度研究」…'"
          @send="send"
          @stop="stopRunning"
        />
        <p class="foot-note">深度研究每轮最多调用 30 次工具；回答可能有误，请核对来源。</p>
      </div>
    </section>

    <SidePanel v-if="!narrow" v-model:tab="panelTab" :turn="panelTurn" />

    <!-- 窄屏：侧栏与右侧面板收为抽屉 -->
    <div v-if="narrow && drawer" class="drawer-layer">
      <div class="backdrop" @click="drawer = null"></div>
      <div class="drawer" :class="drawer === 'sessions' ? 'left' : 'right'" role="dialog" aria-modal="true" :aria-label="drawer === 'sessions' ? '会话' : '研究面板'" @keydown.esc="drawer = null">
        <div class="drawer-head">
          <span>{{ drawer === "sessions" ? "会话" : "研究面板" }}</span>
          <button class="drawer-close" type="button" data-action="close-drawer" aria-label="关闭" @click="drawer = null">×</button>
        </div>
        <SessionSidebar
          v-if="drawer === 'sessions'"
          :sessions="sessions"
          :active-id="currentId"
          :loading="sessionsLoading"
          @create="newChat"
          @select="selectSession"
          @rename="renameSession"
          @remove="removeSession"
        />
        <SidePanel v-else v-model:tab="panelTab" :turn="panelTurn" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 对话页（布局 B）：左侧会话列表 | 中间对话（用户气泡在右、AI 在左）与输入框 | 右侧研究面板（进度/来源/报告）。
// - 打开会话：listTurns → seedTurns，再从 cursor 0 订阅会话事件流（重放即可还原全部步骤）；切换会话时关闭旧流。
// - 新对话的首条消息：createSession → 路由到 #/s/<id> → sendMessage。
// - 有进行中的轮次（queued/running/stopping）时禁用发送（契约裁定 I：服务端会 409 turn_in_progress），提示"先停止当前研究"。
// - 会话 restoring，或向 frozen/evicted 会话发送中：顶部显示"正在恢复对话…"。
// - 401 → emit("unauthorized")；其他错误只显示面向用户的文案。
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { ApiError, newRequestId } from "../api/client";
import type { Answer, ChatSession, Turn, TurnAction } from "../api/chat";
import type { EventStream } from "../api/sse";
import BrandMark from "../components/BrandMark.vue";
import Composer from "../components/chat/Composer.vue";
import MessageBubble from "../components/chat/MessageBubble.vue";
import SessionSidebar from "../components/chat/SessionSidebar.vue";
import SidePanel from "../components/chat/SidePanel.vue";
import TurnView from "../components/chat/TurnView.vue";
import { activeResearchTurn, applyEvent, chatErrorMessage, emptyChat, recordAnswers, seedTurns } from "../lib/chat";
import type { ChatState } from "../lib/chat";
import { isUnauthorized } from "../lib/research";
import { homeHref, navigate, sessionHref } from "../lib/router";
import { useUserServices } from "../lib/userServices";

type Tab = "progress" | "sources" | "report";

const props = withDefaults(defineProps<{ sessionId?: string }>(), { sessionId: undefined });
const emit = defineEmits<{ unauthorized: [] }>();

const { chat, watchSession } = useUserServices();

const EXAMPLES = ["固态电池的产业化进展和主要瓶颈", "用三句话解释什么是 RAG", "2026 年主流大模型沙箱方案对比"];
const RUNNING = new Set(["queued", "running", "stopping"]);
const WAKING = new Set(["frozen", "evicted"]);
const NARROW_QUERY = "(max-width: 900px)";

const sessions = ref<ChatSession[]>([]);
const sessionsLoading = ref(false);
const currentId = ref<string | undefined>(undefined);
const state = ref<ChatState>(emptyChat());
const turnsLoading = ref(false);
const notFound = ref("");
const sending = ref(false);
const error = ref("");
const busyTurns = ref(new Set<string>());
const panelTab = ref<Tab>("progress");
const panelTurnId = ref<string | null>(null);
const drawer = ref<"sessions" | "panel" | null>(null);
const composer = ref<InstanceType<typeof Composer> | null>(null);
const scroller = ref<HTMLElement | null>(null);

let stream: EventStream | null = null;
let generation = 0;
let pendingSend: { key: string; requestId: string } | null = null;

// ---- 窄屏 ----

const narrow = ref(false);
let mql: MediaQueryList | null = null;
function onMedia(e: MediaQueryListEvent): void {
  narrow.value = e.matches;
  if (!e.matches) drawer.value = null;
}

// ---- 派生状态 ----

const runningTurn = computed(() => {
  const ts = state.value.turns;
  for (let i = ts.length - 1; i >= 0; i--) if (RUNNING.has(ts[i]!.status)) return ts[i];
  return undefined;
});

const restoring = computed(() => state.value.sessionStatus === "restoring" || (sending.value && WAKING.has(state.value.sessionStatus)));

const activeResearch = computed(() => activeResearchTurn(state.value));
const panelTurn = computed(() => {
  const id = panelTurnId.value;
  return (id && state.value.turns.find((t) => t.turnId === id)) || activeResearch.value;
});

const title = computed(() => {
  if (!currentId.value) return "新对话";
  const s = sessions.value.find((x) => x.session_id === currentId.value);
  return s?.title.trim() || state.value.turns[0]?.userText.slice(0, 40) || "新对话";
});

// 新的研究轮次出现时，面板跟随它。
watch(
  () => activeResearch.value?.turnId,
  () => {
    panelTurnId.value = null;
  },
);

// ---- 错误 ----

function fail(e: unknown): void {
  if (isUnauthorized(e)) {
    emit("unauthorized");
    return;
  }
  error.value = chatErrorMessage(e);
}

// ---- 会话列表 ----

async function loadSessions(): Promise<void> {
  sessionsLoading.value = true;
  try {
    const all: ChatSession[] = [];
    let after: string | undefined;
    for (let page = 0; page < 20; page++) {
      const r = await chat.listSessions(after ? { after } : {});
      all.push(...r.sessions);
      if (!r.next) break;
      after = r.next;
    }
    sessions.value = all;
    const cur = all.find((s) => s.session_id === currentId.value);
    if (cur && state.value.cursor === 0 && state.value.sessionStatus === "idle") state.value = { ...state.value, sessionStatus: cur.state };
  } catch (e) {
    fail(e);
  } finally {
    sessionsLoading.value = false;
  }
}

function upsertSession(s: ChatSession): void {
  const i = sessions.value.findIndex((x) => x.session_id === s.session_id);
  const next = sessions.value.slice();
  if (i >= 0) next[i] = s;
  else next.unshift(s);
  sessions.value = next;
}

// ---- 打开会话 ----

function closeStream(): void {
  stream?.close();
  stream = null;
}

function openSession(id: string | undefined): void {
  closeStream();
  generation++;
  currentId.value = id;
  const known = id ? sessions.value.find((s) => s.session_id === id) : undefined;
  state.value = { ...emptyChat(), sessionStatus: known?.state ?? "idle" };
  panelTurnId.value = null;
  panelTab.value = "progress";
  notFound.value = "";
  error.value = "";
  busyTurns.value = new Set();
  stick = true;
  if (id) void loadSession(id, generation);
}

async function loadSession(id: string, gen: number): Promise<void> {
  turnsLoading.value = true;
  try {
    const { turns } = await chat.listTurns(id);
    if (gen !== generation) return;
    state.value = seedTurns(state.value, turns);
    subscribe(id, gen);
  } catch (e) {
    if (gen !== generation) return;
    if (e instanceof ApiError && (e.code === "session_not_found" || e.status === 404)) {
      notFound.value = chatErrorMessage(e);
      return;
    }
    fail(e);
  } finally {
    if (gen === generation) turnsLoading.value = false;
  }
}

function subscribe(id: string, gen: number): void {
  const s = watchSession({
    sessionId: id,
    cursor: 0,
    onEvent: (ev) => {
      if (gen !== generation) return;
      state.value = applyEvent(state.value, ev);
      if (ev.type === "session_state") syncSessionState(id, state.value.sessionStatus);
    },
  });
  stream = s;
  void s.done.then((o) => {
    if (gen !== generation || o.kind !== "error") return;
    if (isUnauthorized(o.error)) emit("unauthorized");
    else error.value = "与服务器的连接已断开，请刷新页面";
  });
}

function syncSessionState(id: string, st: string): void {
  const s = sessions.value.find((x) => x.session_id === id);
  if (s && s.state !== st) upsertSession({ ...s, state: st as ChatSession["state"] });
  if (st === "closed" && currentId.value === id) {
    sessions.value = sessions.value.filter((x) => x.session_id !== id);
    newChat();
  }
}

watch(
  () => props.sessionId,
  (id) => {
    if (id !== currentId.value) openSession(id);
  },
);

// ---- 侧栏操作 ----

function newChat(): void {
  drawer.value = null;
  navigate(homeHref());
  if (currentId.value !== undefined || notFound.value) openSession(undefined);
  void nextTick(() => composer.value?.focus());
}

function selectSession(id: string): void {
  drawer.value = null;
  navigate(sessionHref(id));
  if (id !== currentId.value) openSession(id);
}

async function renameSession(id: string, newTitle: string): Promise<void> {
  try {
    upsertSession(await chat.renameSession(id, newTitle));
  } catch (e) {
    fail(e);
  }
}

async function removeSession(id: string): Promise<void> {
  try {
    await chat.deleteSession(id);
    sessions.value = sessions.value.filter((s) => s.session_id !== id);
    if (id === currentId.value) newChat();
  } catch (e) {
    fail(e);
  }
}

// ---- 发送与轮次操作 ----

async function send(payload: { text: string; deepResearch: boolean }): Promise<void> {
  if (sending.value || runningTurn.value) return;
  sending.value = true;
  error.value = "";
  try {
    let id = currentId.value;
    if (!id) {
      const s = await chat.createSession();
      id = s.session_id;
      upsertSession(s);
      openSession(id);
      navigate(sessionHref(id));
    }
    // 网络失败后用户重试同一内容时复用 request_id（服务端幂等，不会重复建轮次）。
    const key = `${id}\n${payload.deepResearch}\n${payload.text}`;
    if (pendingSend?.key !== key) pendingSend = { key, requestId: newRequestId() };
    const gen = generation;
    const r = await chat.sendMessage(id, payload.text, payload.deepResearch, pendingSend.requestId);
    pendingSend = null;
    composer.value?.clear();
    if (gen === generation && !state.value.turns.some((t) => t.turnId === r.turn_id)) {
      const stub: Turn = {
        turn_id: r.turn_id,
        turn_index: r.turn_index,
        text: payload.text,
        deep_research: payload.deepResearch,
        status: "queued",
        restorable: false,
        tool_calls_used: 0,
        tool_call_limit: 0,
        created_at: new Date().toISOString(),
      };
      state.value = seedTurns(state.value, [stub]);
    }
    stick = true;
    void loadSessions(); // 标题（首条消息）与最近活动时间
  } catch (e) {
    fail(e);
  } finally {
    sending.value = false;
  }
}

async function withTurn(turnId: string, fn: () => Promise<unknown>): Promise<void> {
  if (busyTurns.value.has(turnId)) return;
  busyTurns.value = new Set(busyTurns.value).add(turnId);
  error.value = "";
  try {
    await fn();
  } catch (e) {
    fail(e);
  } finally {
    const next = new Set(busyTurns.value);
    next.delete(turnId);
    busyTurns.value = next;
  }
}

function control(turnId: string, action: TurnAction): void {
  void withTurn(turnId, () => chat.control(turnId, action));
}

function stopRunning(): void {
  const t = runningTurn.value;
  if (t && t.status !== "stopping") control(t.turnId, "stop");
}

function restore(turnId: string): void {
  void withTurn(turnId, async () => {
    await chat.restore(turnId);
    stick = true;
  });
}

function answer(turnId: string, answers: Answer[]): void {
  void withTurn(turnId, async () => {
    await chat.answer(turnId, answers);
    const texts = answers.map((a) => a.choice ?? a.other ?? "");
    state.value = recordAnswers(state.value, turnId, texts);
  });
}

function openPanel(turnId: string, tab: Tab): void {
  panelTurnId.value = turnId;
  panelTab.value = tab;
  if (narrow.value) drawer.value = "panel";
}

// ---- 自动滚动：停在底部时跟随新内容 ----

let stick = true;
function onScroll(): void {
  const el = scroller.value;
  if (!el) return;
  stick = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
}
watch(
  () => [state.value.cursor, state.value.turns.length],
  () => {
    if (!stick) return;
    void nextTick(() => {
      const el = scroller.value;
      if (el) el.scrollTop = el.scrollHeight;
    });
  },
);

onMounted(() => {
  if (typeof window.matchMedia === "function") {
    mql = window.matchMedia(NARROW_QUERY);
    narrow.value = mql.matches;
    mql.addEventListener?.("change", onMedia);
  }
  void loadSessions();
  openSession(props.sessionId);
});

onBeforeUnmount(() => {
  generation++;
  closeStream();
  mql?.removeEventListener?.("change", onMedia);
});
</script>

<style scoped>
.chat-view {
  flex: 1;
  min-height: 0;
  display: grid;
  grid-template-columns: 264px minmax(0, 1fr) 340px;
  background: #fff;
}
.chat-view.narrow {
  grid-template-columns: minmax(0, 1fr);
}
@media (max-width: 1180px) {
  .chat-view:not(.narrow) {
    grid-template-columns: 232px minmax(0, 1fr) 300px;
  }
}
.chat-view > :deep(.side-panel) {
  min-height: 0;
  overflow-y: auto;
  border-left: 1px solid #e8e8ee;
  background: #fcfcfd;
}

/* ---- 中间：对话 ---- */
.conv {
  position: relative;
  display: flex;
  flex-direction: column;
  min-width: 0;
  min-height: 0;
}
.conv-head {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 24px;
  border-bottom: 1px solid #f0f0f4;
}
.narrow .conv-head {
  padding: 10px 12px;
}
.conv-title {
  flex: 1;
  min-width: 0;
  margin: 0;
  font-size: 0.95rem;
  font-weight: 650;
  color: #111827;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.narrow .conv-title {
  text-align: center;
}
.head-btn {
  flex: none;
  padding: 5px 10px;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fff;
  font: inherit;
  font-size: 0.8rem;
  color: #374151;
  cursor: pointer;
}
.head-btn:hover {
  background: #f9fafb;
}
.banner {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 7px 16px;
  font-size: 0.82rem;
}
.banner.info {
  background: #f5f3ff;
  color: #5b21b6;
  border-bottom: 1px solid #ede9fe;
}
.banner.warn {
  background: #fffbeb;
  color: #92400e;
  border-bottom: 1px solid #fef3c7;
}
.spinner {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  border: 2px solid #c4b5fd;
  border-top-color: #6d28d9;
  animation: spin 0.8s linear infinite;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
.scroll {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  scroll-behavior: smooth;
}
.msgs {
  max-width: 820px;
  margin: 0 auto;
  padding: 28px 28px 16px;
  display: flex;
  flex-direction: column;
  gap: 18px;
}
.narrow .msgs {
  padding: 18px 14px 12px;
}
.loading-note {
  text-align: center;
  color: #9ca3af;
  font-size: 0.85rem;
  padding: 40px 0;
}

/* 空状态 */
.empty-state {
  margin: 9vh auto 0;
  max-width: 560px;
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  gap: 10px;
}
.hero-mark :deep(.brand-mark) {
  width: 40px;
  height: 40px;
}
.hero-title {
  margin: 4px 0 0;
  font-size: 1.6rem;
  font-weight: 750;
  letter-spacing: -0.02em;
  color: #111827;
}
.hero-sub {
  margin: 0;
  font-size: 0.92rem;
  line-height: 1.7;
  color: #6b7280;
}
.examples {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: 8px;
  margin-top: 10px;
}
.chip {
  padding: 7px 14px;
  border: 1px solid #e5e7eb;
  border-radius: 999px;
  background: #fff;
  font: inherit;
  font-size: 0.84rem;
  color: #374151;
  cursor: pointer;
  transition: all 0.15s;
}
.chip:hover {
  border-color: #c4b5fd;
  background: #faf8ff;
  color: #5b21b6;
}

/* 输入区 */
.dock {
  width: 100%;
  max-width: 820px;
  margin: 0 auto;
  padding: 8px 28px 12px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.narrow .dock {
  padding: 6px 10px 10px;
}
.chat-error {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 10px;
  border: 1px solid #fecaca;
  background: #fef2f2;
  color: #991b1b;
  font-size: 0.85rem;
}
.chat-error span {
  flex: 1;
}
.x {
  border: 0;
  background: transparent;
  color: inherit;
  font-size: 1.1rem;
  line-height: 1;
  cursor: pointer;
}
.foot-note {
  margin: 0;
  text-align: center;
  font-size: 0.72rem;
  color: #9ca3af;
}

/* ---- 窄屏抽屉 ---- */
.drawer-layer {
  position: fixed;
  inset: 0;
  z-index: 30;
}
.backdrop {
  position: absolute;
  inset: 0;
  background: rgba(17, 24, 39, 0.35);
  animation: fade 0.15s ease-out;
}
.drawer {
  position: absolute;
  top: 0;
  bottom: 0;
  width: min(86vw, 340px);
  display: flex;
  flex-direction: column;
  background: #fff;
  box-shadow: 0 0 32px rgba(17, 24, 39, 0.2);
  overflow-y: auto;
}
.drawer.left {
  left: 0;
  animation: in-left 0.18s ease-out;
}
.drawer.right {
  right: 0;
  animation: in-right 0.18s ease-out;
}
.drawer > :deep(.sidebar),
.drawer > :deep(.side-panel) {
  flex: 1;
  border: 0;
}
.drawer-head {
  flex: none;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 10px 10px 16px;
  border-bottom: 1px solid #f0f0f4;
  font-size: 0.9rem;
  font-weight: 650;
  color: #111827;
}
.drawer-close {
  width: 30px;
  height: 30px;
  border: 0;
  border-radius: 8px;
  background: transparent;
  font-size: 1.2rem;
  color: #6b7280;
  cursor: pointer;
}
@keyframes fade {
  from {
    opacity: 0;
  }
}
@keyframes in-left {
  from {
    transform: translateX(-100%);
  }
}
@keyframes in-right {
  from {
    transform: translateX(100%);
  }
}
</style>
