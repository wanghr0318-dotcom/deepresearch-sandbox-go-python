<template>
  <nav class="sidebar" aria-label="对话列表">
    <button class="new-chat" type="button" data-action="new-chat" @click="emit('create')">
      <span class="plus" aria-hidden="true">＋</span>新对话
    </button>

    <div class="label">最近对话</div>
    <div v-if="loading && !sessions.length" class="side-empty">加载中…</div>
    <div v-else-if="!sessions.length" class="side-empty">还没有对话，发一条消息开始吧。</div>

    <ul v-else class="list">
      <li v-for="s in sorted" :key="s.session_id" class="item" :class="{ on: s.session_id === activeId }" :data-session="s.session_id">
        <!-- 重命名：行内输入，Enter 保存、Esc 取消 -->
        <div v-if="editing === s.session_id" class="edit">
          <input
            ref="renameInput"
            v-model="draft"
            name="title"
            class="rename"
            type="text"
            aria-label="对话标题"
            :maxlength="TITLE_MAX"
            @keydown.enter.prevent="saveRename"
            @keydown.esc.prevent.stop="cancelRename"
            @blur="blurRename"
          />
          <div v-if="renameError" class="field-err" role="alert">{{ renameError }}</div>
        </div>

        <!-- 删除确认 -->
        <div v-else-if="confirming === s.session_id" class="confirm" role="group" aria-label="确认删除">
          <span class="confirm-text">删除这个对话？</span>
          <span class="confirm-btns">
            <button class="mini danger" type="button" data-action="confirm-delete" @click="confirmDelete(s.session_id)">删除</button>
            <button class="mini" type="button" data-action="cancel-delete" @click="confirming = null">取消</button>
          </span>
        </div>

        <template v-else>
          <button
            class="select"
            type="button"
            data-action="select"
            :aria-current="s.session_id === activeId ? 'page' : undefined"
            :title="titleOf(s)"
            @click="emit('select', s.session_id)"
          >
            <span class="title" :class="{ untitled: !s.title.trim() }">{{ titleOf(s) }}</span>
            <span v-if="s.state === 'running'" class="live" title="进行中" aria-label="进行中"></span>
          </button>
          <button
            class="menu-btn"
            type="button"
            data-action="menu"
            aria-label="更多操作"
            aria-haspopup="menu"
            :aria-expanded="menuFor === s.session_id ? 'true' : 'false'"
            @click.stop="toggleMenu(s.session_id)"
          >
            ⋯
          </button>
          <div v-if="menuFor === s.session_id" class="menu" role="menu" @click.stop>
            <button type="button" role="menuitem" data-action="rename" @click="startRename(s)">重命名</button>
            <button type="button" role="menuitem" class="danger" data-action="delete" @click="startDelete(s.session_id)">删除</button>
          </div>
        </template>
      </li>
    </ul>
  </nav>
</template>

<script setup lang="ts">
// 会话侧栏（布局 B 左栏）："新对话"、按最近活动排序的会话列表（无标题显示"新对话"）、
// 每项菜单：重命名（行内输入，Enter 保存、Esc 取消、失焦时有效则保存）、删除（行内确认）。
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from "vue";
import { TITLE_MAX, validateTitle } from "../../api/chat";
import type { ChatSession } from "../../api/chat";

const props = withDefaults(defineProps<{ sessions: ChatSession[]; activeId?: string; loading: boolean }>(), { activeId: undefined });
const emit = defineEmits<{ create: []; select: [id: string]; rename: [id: string, title: string]; remove: [id: string] }>();

const menuFor = ref<string | null>(null);
const editing = ref<string | null>(null);
const confirming = ref<string | null>(null);
const draft = ref("");
const renameError = ref("");
const renameInput = ref<HTMLInputElement[] | null>(null);

function time(s: ChatSession): number {
  const t = Date.parse(s.last_active_at || s.created_at);
  return Number.isNaN(t) ? 0 : t;
}

const sorted = computed(() => props.sessions.slice().sort((a, b) => time(b) - time(a)));

function titleOf(s: ChatSession): string {
  return s.title.trim() || "新对话";
}

function toggleMenu(id: string): void {
  menuFor.value = menuFor.value === id ? null : id;
}

function startRename(s: ChatSession): void {
  menuFor.value = null;
  confirming.value = null;
  editing.value = s.session_id;
  draft.value = s.title;
  renameError.value = "";
  void nextTick(() => {
    const el = renameInput.value?.[0];
    el?.focus();
    el?.select();
  });
}

function cancelRename(): void {
  editing.value = null;
  renameError.value = "";
}

function saveRename(): void {
  const id = editing.value;
  if (!id) return;
  const msg = validateTitle(draft.value);
  if (msg) {
    renameError.value = msg;
    return;
  }
  editing.value = null;
  renameError.value = "";
  const title = draft.value.trim();
  if (title !== props.sessions.find((s) => s.session_id === id)?.title) emit("rename", id, title);
}

function blurRename(): void {
  // 失焦：有效且有改动则保存，否则放弃（Enter/Esc 已先把 editing 置空，这里不会重复处理）。
  if (!editing.value) return;
  if (validateTitle(draft.value)) cancelRename();
  else saveRename();
}

function startDelete(id: string): void {
  menuFor.value = null;
  editing.value = null;
  confirming.value = id;
}

function confirmDelete(id: string): void {
  confirming.value = null;
  emit("remove", id);
}

function closeMenu(): void {
  menuFor.value = null;
}

function onKey(e: KeyboardEvent): void {
  if (e.key === "Escape") {
    menuFor.value = null;
    confirming.value = null;
  }
}

onMounted(() => {
  document.addEventListener("click", closeMenu);
  document.addEventListener("keydown", onKey);
});
onBeforeUnmount(() => {
  document.removeEventListener("click", closeMenu);
  document.removeEventListener("keydown", onKey);
});
</script>

<style scoped>
.sidebar {
  display: flex;
  flex-direction: column;
  gap: 4px;
  height: 100%;
  min-height: 0;
  padding: 14px 10px;
  background: #f7f7f9;
  border-right: 1px solid #e8e8ee;
}
.new-chat {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 9px 12px;
  border: 0;
  border-radius: 10px;
  background: #111827;
  color: #fff;
  font: inherit;
  font-size: 0.88rem;
  font-weight: 650;
  cursor: pointer;
  transition: background 0.15s, transform 0.1s;
}
.new-chat:hover {
  background: #2d3343;
}
.new-chat:active {
  transform: translateY(1px);
}
.plus {
  font-weight: 700;
}
.label {
  margin: 14px 8px 4px;
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  color: #9ca3af;
}
.side-empty {
  padding: 8px;
  font-size: 0.82rem;
  color: #9ca3af;
  line-height: 1.6;
}
.list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
  overflow-y: auto;
  min-height: 0;
}
.item {
  position: relative;
  display: flex;
  align-items: center;
  border-radius: 8px;
  transition: background 0.12s;
}
.item:hover {
  background: #ececf2;
}
.item.on {
  background: #e9e5fb;
}
.select {
  flex: 1;
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 4px 8px 10px;
  border: 0;
  background: transparent;
  font: inherit;
  font-size: 0.875rem;
  color: #374151;
  text-align: left;
  cursor: pointer;
}
.item.on .select {
  color: #3b0764;
  font-weight: 600;
}
.title {
  flex: 1;
  min-width: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.title.untitled {
  color: #6b7280;
}
.live {
  flex: none;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: #7c3aed;
  animation: pulse 1.6s ease-in-out infinite;
}
@keyframes pulse {
  50% {
    opacity: 0.35;
  }
}
.menu-btn {
  flex: none;
  width: 28px;
  height: 28px;
  margin-right: 3px;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #6b7280;
  font-size: 1rem;
  line-height: 1;
  cursor: pointer;
  opacity: 0;
  transition: opacity 0.12s, background 0.12s;
}
.item:hover .menu-btn,
.item.on .menu-btn,
.menu-btn:focus-visible,
.menu-btn[aria-expanded="true"] {
  opacity: 1;
}
.menu-btn:hover {
  background: #dcdce6;
}
@media (hover: none) {
  .menu-btn {
    opacity: 1;
  }
}
.menu {
  position: absolute;
  top: calc(100% + 2px);
  right: 4px;
  z-index: 10;
  min-width: 120px;
  padding: 4px;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #fff;
  box-shadow: 0 8px 24px rgba(17, 24, 39, 0.12);
  display: flex;
  flex-direction: column;
}
.menu button {
  padding: 7px 10px;
  border: 0;
  border-radius: 6px;
  background: transparent;
  font: inherit;
  font-size: 0.85rem;
  color: #374151;
  text-align: left;
  cursor: pointer;
}
.menu button:hover {
  background: #f3f4f6;
}
.menu button.danger {
  color: #b91c1c;
}
.menu button.danger:hover {
  background: #fef2f2;
}
.edit {
  flex: 1;
  padding: 4px;
}
.rename {
  width: 100%;
  padding: 6px 8px;
  border: 1px solid #a78bfa;
  border-radius: 7px;
  outline: none;
  font: inherit;
  font-size: 0.86rem;
  box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.12);
}
.field-err {
  margin: 4px 2px 0;
  font-size: 0.75rem;
  color: #b91c1c;
}
.confirm {
  flex: 1;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  padding: 7px 8px 7px 10px;
  border-radius: 8px;
  background: #fef2f2;
}
.confirm-text {
  flex: 1;
  font-size: 0.82rem;
  color: #991b1b;
  white-space: nowrap;
}
.confirm-btns {
  display: flex;
  gap: 4px;
}
.mini {
  padding: 3px 9px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  background: #fff;
  font: inherit;
  font-size: 0.78rem;
  color: #374151;
  cursor: pointer;
}
.mini.danger {
  border-color: #dc2626;
  background: #dc2626;
  color: #fff;
}
.mini.danger:hover {
  background: #b91c1c;
}
</style>
