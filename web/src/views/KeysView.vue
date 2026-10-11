<template>
  <div class="gw-page">
    <header class="gw-head">
      <div>
        <h1 class="gw-title">API 密钥</h1>
        <p class="gw-sub">用 API 密钥从程序调用运行时 API 或模型 API（OpenAI / Anthropic 格式）。密钥只代表你本人，不能管理密钥、成员或预算。</p>
      </div>
      <span class="spacer"></span>
      <button ref="newKeyButton" class="btn primary" type="button" data-action="new-key" @click="openForm">新建密钥</button>
    </header>

    <div v-if="invites > 0" class="notice" data-testid="invite-banner">
      你收到了 {{ invites }} 个组织邀请，<a class="link" :href="orgHref()">去查看</a>
    </div>

    <form v-if="formOpen" class="panel gw-form" data-testid="key-form" novalidate @submit.prevent="create">
      <h2 class="panel-title">新建密钥</h2>
      <label class="field">
        名称
        <input v-model="name" name="key-name" maxlength="64" placeholder="例如：ci 或 本地开发" />
      </label>
      <fieldset class="scopes">
        <legend>权限范围</legend>
        <label v-for="s in SCOPES" :key="s" class="check">
          <input v-model="scopes" type="checkbox" :value="s" :name="`scope-${s}`" />
          {{ SCOPE_LABELS[s] }}
        </label>
      </fieldset>
      <label class="field">
        有效期
        <select v-model="expiry" name="expiry">
          <option value="">永不过期</option>
          <option value="30">30 天</option>
          <option value="90">90 天</option>
          <option value="365">1 年</option>
        </select>
      </label>
      <div v-if="formError" class="user-error" role="alert" data-testid="form-error">{{ formError }}</div>
      <div class="row">
        <button class="btn primary" type="submit" :disabled="busy">{{ busy ? "创建中…" : "创建" }}</button>
        <button class="btn" type="button" @click="formOpen = false">取消</button>
      </div>
    </form>

    <section class="panel">
      <h2 class="panel-title">我的密钥</h2>
      <div v-if="error" class="user-error" role="alert" data-testid="list-error">{{ error }}</div>
      <div v-if="loading && keys.length === 0" class="empty">加载中…</div>
      <div v-else-if="keys.length === 0 && !error" class="empty" data-testid="no-keys">还没有密钥。</div>
      <div v-else-if="keys.length > 0" class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>名称</th>
              <th>ID</th>
              <th>权限</th>
              <th>创建</th>
              <th>最近使用</th>
              <th>过期</th>
              <th>状态</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="k in keys" :key="k.id" :data-key="k.id">
              <td>{{ k.name }}<span v-if="k.username !== me?.username" class="muted owner">（{{ k.username }}）</span></td>
              <td class="mono">{{ k.id }}</td>
              <td>{{ scopeNames(k) }}</td>
              <td>{{ formatDateTime(k.created_at) }}</td>
              <td>{{ k.last_used_at ? formatDateTime(k.last_used_at) : "从未使用" }}</td>
              <td>{{ k.expires_at ? formatDateTime(k.expires_at) : "永不" }}</td>
              <td>
                <span class="badge" :class="statusOf(k).cls">{{ statusOf(k).label }}</span>
              </td>
              <td class="actions">
                <template v-if="!k.revoked_at">
                  <span v-if="confirming === k.id" class="confirm" role="group" aria-label="确认吊销">
                    <span class="muted">吊销后立即失效，无法恢复。</span>
                    <button class="btn small danger" type="button" data-action="confirm-revoke" :disabled="busy" @click="revoke(k.id)">确认吊销</button>
                    <button class="btn small" type="button" data-action="cancel-revoke" @click="confirming = ''">取消</button>
                  </span>
                  <button v-else class="btn small danger" type="button" data-action="revoke" @click="confirming = k.id">吊销</button>
                </template>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <div v-if="created" class="backdrop">
      <div
        ref="dialog"
        class="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="key-dialog-title"
        tabindex="-1"
        data-testid="secret-dialog"
        @keydown.tab="trapFocus"
      >
        <h2 id="key-dialog-title" class="d-title">密钥「{{ created.name }}」已创建</h2>
        <p class="warn" role="alert">
          这是唯一一次显示完整密钥。请立即复制并妥善保存；关闭后无法再次查看，遗失只能吊销后重新创建。
        </p>
        <code class="secret mono" data-testid="secret">{{ created.key }}</code>
        <div class="row">
          <button class="btn primary" type="button" data-action="copy" @click="copySecret">{{ copied ? "已复制" : "复制" }}</button>
          <span v-if="copyError" class="muted">{{ copyError }}</span>
          <span class="spacer"></span>
          <button class="btn" type="button" data-action="close-secret" @click="closeSecret">我已保存，关闭</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import type { ApiKey, Me, Scope } from "../api/gateway";
import { SCOPE_LABELS, formatDateTime, gatewayErrorMessage, isUnauthorized } from "../lib/gateway";
import { useGatewayServices } from "../lib/gatewayServices";
import { orgHref } from "../lib/router";

const emit = defineEmits<{ unauthorized: [] }>();
const { api, copy, now } = useGatewayServices();

const SCOPES: Scope[] = ["models", "runtime"];

const me = ref<Me | null>(null);
const keys = ref<ApiKey[]>([]);
const loading = ref(false);
const error = ref("");
const invites = ref(0);

const formOpen = ref(false);
const name = ref("");
const scopes = ref<Scope[]>(["models"]);
const expiry = ref("");
const formError = ref("");
const busy = ref(false);
const confirming = ref("");

// 完整密钥只保存在这个对话框的状态里；关闭即清空。
const created = ref<{ name: string; key: string } | null>(null);
const copied = ref(false);
const copyError = ref("");
const dialog = ref<HTMLElement | null>(null);
const newKeyButton = ref<HTMLButtonElement | null>(null);

// 复审 N3：对话框打开时 Esc 在任何位置都关闭它；Tab 只在对话框内循环；关闭后焦点回到「新建密钥」。
function onWindowKey(e: KeyboardEvent): void {
  if (e.key === "Escape" && created.value) {
    e.preventDefault();
    closeSecret();
  }
}

function trapFocus(e: KeyboardEvent): void {
  const root = dialog.value;
  if (!root) return;
  const items = Array.from(root.querySelectorAll<HTMLElement>("button:not([disabled]), [href], input, select, textarea, [tabindex]:not([tabindex='-1'])"));
  if (items.length === 0) return;
  const first = items[0]!;
  const last = items[items.length - 1]!;
  const active = document.activeElement;
  if (e.shiftKey && (active === first || active === root)) {
    e.preventDefault();
    last.focus();
  } else if (!e.shiftKey && active === last) {
    e.preventDefault();
    first.focus();
  }
}

watch(
  () => created.value !== null,
  (open) => {
    if (open) window.addEventListener("keydown", onWindowKey);
    else window.removeEventListener("keydown", onWindowKey);
  },
);

onBeforeUnmount(() => window.removeEventListener("keydown", onWindowKey));

function fail(e: unknown): string {
  if (isUnauthorized(e)) emit("unauthorized");
  return gatewayErrorMessage(e);
}

function scopeNames(k: ApiKey): string {
  return k.scopes.map((x) => SCOPE_LABELS[x] ?? x).join("、");
}

function statusOf(k: ApiKey): { label: string; cls: string } {
  if (k.revoked_at) return { label: "已吊销", cls: "cancelled" };
  if (k.expires_at && new Date(k.expires_at).getTime() <= now().getTime()) return { label: "已过期", cls: "failed" };
  return { label: "有效", cls: "succeeded" };
}

async function load(): Promise<void> {
  loading.value = true;
  try {
    const [m, ks] = await Promise.all([api.me(), api.listKeys()]);
    me.value = m;
    keys.value = ks;
    error.value = "";
  } catch (e) {
    error.value = fail(e);
  } finally {
    loading.value = false;
  }
  try {
    const invs = await api.listInvites();
    invites.value = invs.filter((i) => i.username === me.value?.username).length;
  } catch {
    invites.value = 0; // 只是提示，失败不影响密钥页
  }
}

function openForm(): void {
  formOpen.value = true;
  formError.value = "";
  name.value = "";
  scopes.value = ["models"];
  expiry.value = "";
}

async function create(): Promise<void> {
  if (busy.value) return;
  const n = name.value.trim();
  if (!n || n.length > 64) {
    formError.value = "名称须为 1–64 个字符";
    return;
  }
  if (scopes.value.length === 0) {
    formError.value = "请至少选择一个权限范围";
    return;
  }
  busy.value = true;
  formError.value = "";
  try {
    const req: { name: string; scopes: Scope[]; expires_at?: string } = { name: n, scopes: [...scopes.value] };
    if (expiry.value) req.expires_at = new Date(now().getTime() + Number(expiry.value) * 86_400_000).toISOString();
    const k = await api.createKey(req);
    created.value = { name: k.name, key: k.key };
    copied.value = false;
    copyError.value = "";
    formOpen.value = false;
    const stored: ApiKey = { id: k.id, name: k.name, org_id: k.org_id, username: k.username, scopes: k.scopes, created_at: k.created_at };
    if (k.expires_at) stored.expires_at = k.expires_at;
    keys.value = [stored, ...keys.value.filter((x) => x.id !== k.id)];
    void nextTick(() => dialog.value?.focus());
  } catch (e) {
    formError.value = fail(e);
  } finally {
    busy.value = false;
  }
}

async function copySecret(): Promise<void> {
  if (!created.value) return;
  try {
    await copy(created.value.key);
    copied.value = true;
    copyError.value = "";
  } catch {
    copyError.value = "无法访问剪贴板，请手动选中复制";
  }
}

function closeSecret(): void {
  created.value = null;
  copied.value = false;
  void nextTick(() => newKeyButton.value?.focus());
}

async function revoke(id: string): Promise<void> {
  busy.value = true;
  try {
    const k = await api.revokeKey(id);
    keys.value = keys.value.map((x) => (x.id === id ? k : x));
    confirming.value = "";
    error.value = "";
  } catch (e) {
    error.value = fail(e);
  } finally {
    busy.value = false;
  }
}

onMounted(() => {
  void load();
});
</script>

<style scoped>
.gw-form {
  display: flex;
  flex-direction: column;
  gap: 12px;
  max-width: 520px;
}
.scopes {
  border: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 0.85rem;
}
.scopes legend {
  font-size: 0.8rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 4px;
}
.check {
  display: flex;
  align-items: center;
  gap: 6px;
}
.owner {
  font-size: 0.75rem;
}
.actions {
  white-space: nowrap;
}
.confirm {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.75rem;
}
.backdrop {
  position: fixed;
  inset: 0;
  z-index: 50;
  background: rgba(17, 24, 39, 0.38);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
.dialog {
  width: min(620px, 100%);
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 18px 48px rgba(17, 24, 39, 0.22);
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.dialog:focus {
  outline: none;
}
.d-title {
  margin: 0;
  font-size: 1rem;
  font-weight: 700;
}
.warn {
  margin: 0;
  border: 1px solid #fde68a;
  background: #fffbeb;
  color: #78350f;
  border-radius: 10px;
  padding: 10px 14px;
  font-size: 0.85rem;
}
.secret {
  display: block;
  padding: 10px 12px;
  background: #f6f8fa;
  border: 1px solid #eaeef2;
  border-radius: 8px;
  word-break: break-all;
  user-select: all;
}
</style>
