<template>
  <div class="gw-page">
    <header class="gw-head">
      <div>
        <h1 class="gw-title">成员</h1>
        <p class="gw-sub">每个用户属于一个组织；加入另一个组织须接受邀请。组织的 owner 管理成员，并为成员与密钥设置限额和预算。</p>
      </div>
    </header>

    <div v-if="error" class="user-error" role="alert" data-testid="org-error">{{ error }}</div>
    <div v-if="message" class="notice" role="status" data-testid="org-message">{{ message }}</div>

    <section v-if="received.length > 0" class="panel" data-testid="received">
      <h2 class="panel-title">收到的邀请</h2>
      <ul class="list">
        <li v-for="inv in received" :key="inv.id" class="item" :data-invite="inv.id">
          <span>
            <strong>{{ inv.invited_by }}</strong> 邀请你以 {{ ROLE_LABELS[inv.role] }} 身份加入组织 <strong>{{ inv.org }}</strong>
            <span class="muted">（{{ formatDateTime(inv.expires_at) }} 前有效）</span>
          </span>
          <span class="spacer"></span>
          <button class="btn small primary" type="button" data-action="accept" :disabled="busy" @click="accept(inv.id)">接受</button>
          <button class="btn small" type="button" data-action="decline" :disabled="busy" @click="decline(inv.id)">拒绝</button>
        </li>
      </ul>
      <p class="muted hint">接受后你将离开当前组织，你在当前组织的 API 密钥会被吊销。</p>
    </section>

    <section v-if="me" class="panel" data-testid="my-org">
      <h2 class="panel-title">我的组织</h2>
      <div class="row">
        <span>
          <strong data-testid="org-name">{{ me.org?.name }}</strong>
          <span class="muted"> · 你的角色：{{ ROLE_LABELS[me.role ?? "member"] }}</span>
          <span v-if="personal" class="muted"> · 个人组织</span>
        </span>
        <span class="spacer"></span>
        <template v-if="!personal">
          <span v-if="confirmLeave" class="confirm" role="group" aria-label="确认离开">
            <span class="muted">离开后回到个人组织，你在本组织的密钥会被吊销。</span>
            <button class="btn small danger" type="button" data-action="confirm-leave" :disabled="busy" @click="leave">确认离开</button>
            <button class="btn small" type="button" @click="confirmLeave = false">取消</button>
          </span>
          <button v-else class="btn small danger" type="button" data-action="leave" @click="confirmLeave = true">离开组织</button>
        </template>
      </div>
      <p v-if="personal && me.role === 'owner'" class="muted hint">个人组织不能邀请其他成员；需要团队组织时请联系运维创建（agentbox org create）。</p>
      <p v-else-if="me.role !== 'owner'" class="muted hint" data-testid="not-owner">只有组织的 owner 可以查看和管理成员。</p>
    </section>

    <template v-if="isOwner">
      <section class="panel" data-testid="members">
        <h2 class="panel-title">成员</h2>
        <table class="data-table">
          <thead><tr><th>用户名</th><th>角色</th><th></th></tr></thead>
          <tbody>
            <tr v-for="m in members" :key="m.username" :data-member="m.username">
              <td>{{ m.username }}<span v-if="m.username === me?.username" class="muted">（你）</span></td>
              <td>{{ ROLE_LABELS[m.role] }}</td>
              <td class="actions">
                <template v-if="m.username !== me?.username">
                  <span v-if="confirmRemove === m.username" class="confirm" role="group" aria-label="确认移出">
                    <button class="btn small danger" type="button" data-action="confirm-remove" :disabled="busy" @click="remove(m.username)">确认移出</button>
                    <button class="btn small" type="button" @click="confirmRemove = ''">取消</button>
                  </span>
                  <button v-else class="btn small danger" type="button" data-action="remove" @click="confirmRemove = m.username">移出</button>
                </template>
              </td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="gw-grid">
        <form class="panel invite" data-testid="invite-form" novalidate @submit.prevent="invite">
          <h2 class="panel-title">邀请成员</h2>
          <label class="field">用户名 <input v-model="inviteName" name="invite-username" maxlength="32" placeholder="对方的用户名" /></label>
          <label class="field">
            角色
            <select v-model="inviteRole" name="invite-role">
              <option value="member">成员</option>
              <option value="owner">owner</option>
            </select>
          </label>
          <div v-if="inviteError" class="user-error" role="alert" data-testid="invite-error">{{ inviteError }}</div>
          <div class="row"><button class="btn primary" type="submit" :disabled="busy">发送邀请</button></div>
          <p class="muted hint">对方接受后才会加入；邀请 7 天内有效。</p>
        </form>

        <section class="panel" data-testid="sent">
          <h2 class="panel-title">待接受的邀请</h2>
          <div v-if="sent.length === 0" class="muted">没有待接受的邀请。</div>
          <ul v-else class="list">
            <li v-for="inv in sent" :key="inv.id" class="item" :data-invite="inv.id">
              <span>{{ inv.username }} <span class="muted">· {{ ROLE_LABELS[inv.role] }} · {{ formatDateTime(inv.expires_at) }} 前</span></span>
              <span class="spacer"></span>
              <button class="btn small danger" type="button" data-action="revoke-invite" :disabled="busy" @click="revokeInvite(inv.id)">撤销</button>
            </li>
          </ul>
        </section>
      </section>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import type { Invite, Me, Membership, Role } from "../api/gateway";
import { ROLE_LABELS, formatDateTime, gatewayErrorMessage, isPersonalOrg, isUnauthorized } from "../lib/gateway";
import { useGatewayServices } from "../lib/gatewayServices";

const emit = defineEmits<{ unauthorized: [] }>();
const { api } = useGatewayServices();

const me = ref<Me | null>(null);
const invites = ref<Invite[]>([]);
const members = ref<Membership[]>([]);
const error = ref("");
const message = ref("");
const busy = ref(false);
const confirmLeave = ref(false);
const confirmRemove = ref("");
const inviteName = ref("");
const inviteRole = ref<Role>("member");
const inviteError = ref("");

const personal = computed(() => isPersonalOrg(me.value?.org?.name));
const isOwner = computed(() => me.value?.role === "owner" && !personal.value);
const received = computed(() => invites.value.filter((i) => i.username === me.value?.username));
const sent = computed(() => invites.value.filter((i) => i.username !== me.value?.username && i.org_id === me.value?.org?.id));

function fail(e: unknown): string {
  if (isUnauthorized(e)) emit("unauthorized");
  return gatewayErrorMessage(e);
}

async function load(): Promise<void> {
  try {
    me.value = await api.me();
    invites.value = await api.listInvites();
    members.value = isOwner.value ? (await api.listMembers()).members : [];
    error.value = "";
  } catch (e) {
    error.value = fail(e);
  }
}

async function act(fn: () => Promise<unknown>, done: string): Promise<boolean> {
  busy.value = true;
  try {
    await fn();
    message.value = done;
    error.value = "";
    await load();
    return true;
  } catch (e) {
    error.value = fail(e);
    message.value = "";
    return false;
  } finally {
    busy.value = false;
  }
}

function accept(id: string): Promise<boolean> {
  return act(() => api.acceptInvite(id), "已加入新组织。你在原组织的密钥已吊销，请在新组织中重新创建。");
}

function decline(id: string): Promise<boolean> {
  return act(() => api.deleteInvite(id), "已拒绝邀请。");
}

async function leave(): Promise<void> {
  const name = me.value?.username;
  if (!name) return;
  if (await act(() => api.removeMember(name), "你已离开组织，回到个人组织。")) confirmLeave.value = false;
}

async function remove(username: string): Promise<void> {
  if (await act(() => api.removeMember(username), `已移出 ${username}。`)) confirmRemove.value = "";
}

function revokeInvite(id: string): Promise<boolean> {
  return act(() => api.deleteInvite(id), "已撤销邀请。");
}

async function invite(): Promise<void> {
  const name = inviteName.value.trim();
  if (!name) {
    inviteError.value = "请输入用户名";
    return;
  }
  busy.value = true;
  try {
    await api.createInvite(name, inviteRole.value);
    inviteError.value = "";
    inviteName.value = "";
    message.value = `已邀请 ${name}，对方接受后加入。`;
    invites.value = await api.listInvites();
  } catch (e) {
    inviteError.value = fail(e);
  } finally {
    busy.value = false;
  }
}

onMounted(() => {
  void load();
});
</script>

<style scoped>
.list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.88rem;
  flex-wrap: wrap;
}
.hint {
  margin: 8px 0 0;
  font-size: 0.78rem;
}
.confirm {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.75rem;
  flex-wrap: wrap;
}
.invite {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.actions {
  white-space: nowrap;
}
</style>
