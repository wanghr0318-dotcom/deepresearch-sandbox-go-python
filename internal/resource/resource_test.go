package resource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/fake"
)

// ---- 测试装置 ----

// rec 是 Store 替身与 provider 包装共享的调用记录，用于断言跨两侧的调用顺序。
type rec struct {
	mu  sync.Mutex
	log []string
}

func (r *rec) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, s)
}

func (r *rec) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.log)
}

func (r *rec) count(s string) int {
	n := 0
	for _, x := range r.snapshot() {
		if x == s {
			n++
		}
	}
	return n
}

// memStore 是 resource.Store 的内存替身：语义与 Plan 4 的事务用例一致（事务性不在此测试）。
type memStore struct {
	rec *rec

	mu         sync.Mutex
	envs       map[string]*Environment
	ended      map[string]bool // 所属 attempt 已有判决
	intents    map[string]Intent
	ranges     []*UIDRange
	quarantine map[string]Quarantine
	fail       map[string]int      // 方法名 → 接下来失败的次数（ErrUnavailable）
	onFail     func(method string) // 每次注入失败后调用（锁外）
}

func newMemStore(r *rec) *memStore {
	return &memStore{
		rec: r, envs: map[string]*Environment{}, ended: map[string]bool{},
		intents: map[string]Intent{}, quarantine: map[string]Quarantine{}, fail: map[string]int{},
	}
}

func (s *memStore) addEnv(envID string, ended bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envs[envID] = &Environment{EnvID: envID, Kind: "task", AttemptID: "att-" + envID, Status: "creating", CleanupState: CleanupNone}
	s.ended[envID] = ended
}

func (s *memStore) failNext(method string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail[method] = n
}

// enter 检查注入的失败；返回非 nil 时调用方直接返回该错误。
func (s *memStore) enter(method string) error {
	s.mu.Lock()
	n := s.fail[method]
	if n == 0 {
		s.mu.Unlock()
		return nil
	}
	s.fail[method] = n - 1
	hook := s.onFail
	s.mu.Unlock()
	if hook != nil {
		hook(method)
	}
	return fmt.Errorf("%w: 注入 %s", persistence.ErrUnavailable, method)
}

func (s *memStore) env(envID string) Environment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.envs[envID]
}

func (s *memStore) intentState(envID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.intents[intentID(envID)].State
}

func (s *memStore) rangeOf(id string) UIDRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.ranges {
		if r.UIDRangeID == id {
			return *r
		}
	}
	return UIDRange{}
}

func (s *memStore) RecordIntent(_ context.Context, i Intent) (Intent, error) {
	if err := s.enter("RecordIntent"); err != nil {
		return Intent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("RecordIntent")
	if cur, ok := s.intents[i.IntentID]; ok {
		return cur, nil
	}
	i.State = IntentPending
	s.intents[i.IntentID] = i
	return i, nil
}

func (s *memStore) ResolveIntent(_ context.Context, id, state string) (Intent, error) {
	if err := s.enter("ResolveIntent"); err != nil {
		return Intent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("ResolveIntent:" + state)
	cur, ok := s.intents[id]
	if !ok {
		return Intent{}, persistence.ErrNotFound
	}
	allowed := map[string]string{IntentPending + ">" + IntentAcquired: "", IntentPending + ">" + IntentFailed: "", IntentAcquired + ">" + IntentReleased: ""}
	if cur.State != state {
		if _, ok := allowed[cur.State+">"+state]; !ok {
			return Intent{}, fmt.Errorf("%w: intent %s -> %s", persistence.ErrConflict, cur.State, state)
		}
		cur.State = state
		s.intents[id] = cur
	}
	return cur, nil
}

func (s *memStore) GetIntent(_ context.Context, id string) (Intent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.intents[id]
	if !ok {
		return Intent{}, persistence.ErrNotFound
	}
	return cur, nil
}

func (s *memStore) SeedUIDRanges(_ context.Context, base, size int64, count int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < count; i++ {
		b := base + int64(i)*size
		id := fmt.Sprintf("uid-%d", b)
		if !slices.ContainsFunc(s.ranges, func(r *UIDRange) bool { return r.UIDRangeID == id }) {
			s.ranges = append(s.ranges, &UIDRange{UIDRangeID: id, Base: b, Size: size, State: "free"})
		}
	}
	return nil
}

func (s *memStore) AssignUIDRange(_ context.Context, envID, alloc string) (UIDRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("AssignUIDRange")
	if _, ok := s.envs[envID]; !ok {
		return UIDRange{}, persistence.ErrNotFound
	}
	for _, r := range s.ranges {
		if r.State == "assigned" && r.OwnerID == envID {
			if r.AllocationID != alloc {
				return UIDRange{}, persistence.ErrConflict
			}
			return *r, nil
		}
	}
	for _, r := range s.ranges {
		if r.State == "free" {
			r.State, r.OwnerID, r.AllocationID = "assigned", envID, alloc
			return *r, nil
		}
	}
	return UIDRange{}, fmt.Errorf("%w: 没有空闲的 UID 范围", persistence.ErrConflict)
}

func (s *memStore) ReleaseUIDRange(_ context.Context, id, alloc string) (UIDRange, error) {
	if err := s.enter("ReleaseUIDRange"); err != nil {
		return UIDRange{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("ReleaseUIDRange")
	for _, r := range s.ranges {
		if r.UIDRangeID != id {
			continue
		}
		switch {
		case r.AllocationID != alloc:
			return UIDRange{}, persistence.ErrConflict
		case r.State == "free":
			return *r, nil
		}
		e := s.envs[r.OwnerID]
		if e == nil || e.StoppedAt == nil || e.CleanupState != CleanupDone {
			return UIDRange{}, fmt.Errorf("%w: 环境尚未停止并完成清理", persistence.ErrConflict)
		}
		r.State, r.OwnerID = "free", ""
		return *r, nil
	}
	return UIDRange{}, persistence.ErrNotFound
}

func (s *memStore) GetUIDRange(_ context.Context, envID string) (UIDRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.ranges {
		if r.State == "assigned" && r.OwnerID == envID {
			return *r, nil
		}
	}
	return UIDRange{}, persistence.ErrNotFound
}

func (s *memStore) MarkStopped(_ context.Context, envID string, at time.Time) (Environment, error) {
	if err := s.enter("MarkStopped"); err != nil {
		return Environment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("MarkStopped")
	e, ok := s.envs[envID]
	if !ok {
		return Environment{}, persistence.ErrNotFound
	}
	if e.StoppedAt == nil {
		e.StoppedAt = &at
	}
	return *e, nil
}

func (s *memStore) UpdateCleanup(_ context.Context, u CleanupUpdate) (Environment, error) {
	if err := s.enter("UpdateCleanup"); err != nil {
		return Environment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("UpdateCleanup:" + u.State)
	e, ok := s.envs[u.EnvID]
	if !ok {
		return Environment{}, persistence.ErrNotFound
	}
	rank := map[string]int{CleanupNone: 0, CleanupPending: 1, CleanupDone: 2}
	switch {
	case e.CleanupTries == u.ExpectedTries+1 && e.CleanupState == u.State && e.CleanupError == u.Error:
		return *e, nil
	case e.CleanupTries != u.ExpectedTries, rank[u.State] < rank[e.CleanupState]:
		return Environment{}, persistence.ErrConflict
	}
	e.CleanupState, e.CleanupTries, e.CleanupError, e.NextRetryAt = u.State, u.ExpectedTries+1, u.Error, u.NextRetryAt
	return *e, nil
}

func (s *memStore) GetEnvironment(_ context.Context, envID string) (Environment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.envs[envID]
	if !ok {
		return Environment{}, persistence.ErrNotFound
	}
	return *e, nil
}

func (s *memStore) ListCleanupCandidates(_ context.Context, now time.Time, limit int) ([]Environment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Environment
	for id, e := range s.envs {
		if e.StoppedAt != nil && e.CleanupState != CleanupDone && s.ended[id] && (e.NextRetryAt == nil || !e.NextRetryAt.After(now)) {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StoppedAt.Before(*out[j].StoppedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *memStore) RecordQuarantine(_ context.Context, q Quarantine) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("RecordQuarantine")
	if _, ok := s.quarantine[q.Path]; !ok {
		s.quarantine[q.Path] = q
	}
	return nil
}

func (s *memStore) MarkQuarantineAlerted(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.add("MarkQuarantineAlerted")
	return nil
}

// tprov 包装 fake provider：记录调用与结果，并提供 Create 的阻塞钩子与 Destroy 的故障注入。
type tprov struct {
	*fake.Provider
	rec *rec

	mu           sync.Mutex
	beforeCreate func(ctx context.Context) error
	destroyErrs  []error
	specs        []provider.EnvSpec
}

func newTProv(r *rec) *tprov {
	idle := func(ctx context.Context, _ provider.ExecSpec, _ io.Reader, _, _ io.Writer) provider.ExitStatus {
		<-ctx.Done()
		return provider.ExitStatus{}
	}
	return &tprov{Provider: fake.New(idle), rec: r}
}

func errName(err error) string {
	for _, e := range []struct {
		err  error
		name string
	}{
		{provider.ErrIncomplete, "ErrIncomplete"}, {provider.ErrForeign, "ErrForeign"},
		{provider.ErrConflict, "ErrConflict"}, {provider.ErrStopUnconfirmed, "ErrStopUnconfirmed"},
		{provider.ErrNotStopped, "ErrNotStopped"}, {context.Canceled, "Canceled"},
	} {
		if errors.Is(err, e.err) {
			return e.name
		}
	}
	if err == nil {
		return "ok"
	}
	return "err"
}

func (p *tprov) Create(ctx context.Context, spec provider.EnvSpec) (provider.EnvInfo, error) {
	p.mu.Lock()
	hook := p.beforeCreate
	p.specs = append(p.specs, spec)
	p.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			p.rec.add("Create:" + errName(err))
			return provider.EnvInfo{}, err
		}
	}
	info, err := p.Provider.Create(ctx, spec)
	p.rec.add("Create:" + errName(err))
	return info, err
}

func (p *tprov) Stop(ctx context.Context, envID string) error {
	err := p.Provider.Stop(ctx, envID)
	p.rec.add("Stop:" + errName(err))
	return err
}

func (p *tprov) Destroy(ctx context.Context, envID string) error {
	p.mu.Lock()
	var injected error
	if len(p.destroyErrs) > 0 {
		injected, p.destroyErrs = p.destroyErrs[0], p.destroyErrs[1:]
	}
	p.mu.Unlock()
	if injected != nil {
		p.rec.add("Destroy:" + errName(injected))
		return injected
	}
	err := p.Provider.Destroy(ctx, envID)
	p.rec.add("Destroy:" + errName(err))
	return err
}

// clock 是可推进的测试时钟。
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const (
	testInstall = "inst-1"
	testUIDBase = 100000
	testUIDSize = 4096
)

type fixture struct {
	rec   *rec
	store *memStore
	prov  *tprov
	clock *clock
	c     *Coordinator
}

// newFixture 返回一个 coordinator：UID 池 4 段，退避为 (try+1) 秒（cleanup 的 next_retry_at 可预测），
// persist 的重试等待由 retryFast 置零。
func newFixture(t *testing.T, retryFast bool) *fixture {
	t.Helper()
	r := &rec{}
	f := &fixture{rec: r, store: newMemStore(r), prov: newTProv(r), clock: &clock{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}}
	backoff := func(try int) time.Duration { return time.Duration(try+1) * time.Second }
	if retryFast {
		backoff = func(int) time.Duration { return 0 }
	}
	f.c = NewCoordinator(f.store, f.prov, Options{
		InstallID: testInstall, UIDBase: testUIDBase, UIDSize: testUIDSize, UIDCount: 4,
		Backoff: backoff, Now: f.clock.now,
	})
	return f
}

func req(envID string) EnvRequest {
	return EnvRequest{EnvID: envID, AttemptID: "att-" + envID, Kind: provider.KindTask, Template: "py311", Limits: provider.Limits{MemoryMax: 1 << 28}}
}

// specFor 是 coordinator 为第一段 UID 范围构造的 spec。
func specFor(envID string) provider.EnvSpec {
	r := req(envID)
	return provider.EnvSpec{EnvID: envID, InstallID: testInstall, Kind: r.Kind, UIDBase: testUIDBase, UIDSize: testUIDSize, Template: r.Template, Limits: r.Limits}
}

func assertLog(t *testing.T, r *rec, want ...string) {
	t.Helper()
	if got := r.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("调用顺序\n got: %v\nwant: %v", got, want)
	}
}

// createStopped 创建并停止 envID，然后清空调用记录。
func (f *fixture) createStopped(t *testing.T, envID string, ended bool) {
	t.Helper()
	ctx := context.Background()
	f.store.addEnv(envID, ended)
	if _, err := f.c.CreateEnv(ctx, req(envID)); err != nil {
		t.Fatal(err)
	}
	if res, err := f.c.StopEnv(ctx, envID); err != nil || !res.Stopped || !res.Recorded {
		t.Fatalf("StopEnv = %+v, %v", res, err)
	}
	f.rec.mu.Lock()
	f.rec.log = nil
	f.rec.mu.Unlock()
}

// ---- CreateEnv ----

// TestCreateEnvOrder：intent 与 UID 范围先于物理操作，成功后 intent 为 acquired；spec 带分配的范围。
func TestCreateEnvOrder(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	info, err := f.c.CreateEnv(context.Background(), req("e1"))
	if err != nil || !info.Complete {
		t.Fatalf("CreateEnv = %+v, %v", info, err)
	}
	assertLog(t, f.rec, "RecordIntent", "AssignUIDRange", "Create:ok", "ResolveIntent:acquired")
	if got := f.prov.specs[0]; got != specFor("e1") {
		t.Fatalf("spec = %+v", got)
	}
	if s := f.store.intentState("e1"); s != IntentAcquired {
		t.Fatalf("intent = %s", s)
	}
}

// TestCreateEnvIncompleteRebuilds：本安装的残留 → Stop → Destroy → 以新的创建重建一次。
func TestCreateEnvIncompleteRebuilds(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	f.prov.InjectResidue(specFor("e1"))
	info, err := f.c.CreateEnv(context.Background(), req("e1"))
	if err != nil || !info.Complete {
		t.Fatalf("CreateEnv = %+v, %v", info, err)
	}
	assertLog(t, f.rec, "RecordIntent", "AssignUIDRange",
		"Create:ErrIncomplete", "Stop:ok", "Destroy:ok", "Create:ok", "ResolveIntent:acquired")
}

// TestCreateEnvForeignQuarantines：归属不明 → 记录隔离、不停止不销毁、intent 保留 pending。
func TestCreateEnvForeignQuarantines(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	f.prov.InjectForeign("e1")
	_, err := f.c.CreateEnv(context.Background(), req("e1"))
	if !errors.Is(err, provider.ErrForeign) {
		t.Fatalf("err = %v, want ErrForeign", err)
	}
	assertLog(t, f.rec, "RecordIntent", "AssignUIDRange", "Create:ErrForeign", "RecordQuarantine")
	// 隔离记录与 provider 扫描报告的资源逐项一致（Layer、Path 原样），coordinator 不构造路径。
	rep, err := f.prov.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var scanned []Quarantine
	for _, it := range rep.Items {
		if it.EnvID == "e1" {
			scanned = append(scanned, Quarantine{Layer: it.Layer, Path: it.Path})
		}
	}
	if len(scanned) == 0 || len(scanned) != len(f.store.quarantine) {
		t.Fatalf("扫描 %+v，隔离 %+v", scanned, f.store.quarantine)
	}
	for _, s := range scanned {
		if q, ok := f.store.quarantine[s.Path]; !ok || q.Layer != s.Layer || q.Path != s.Path {
			t.Fatalf("扫描项 %+v 的隔离记录 = %+v", s, q)
		}
	}
	if s := f.store.intentState("e1"); s != IntentPending {
		t.Fatalf("intent = %s, want pending", s)
	}
}

// TestCreateEnvCanceledKeepsIntentPending：创建中途取消时不能确认无残留，intent 保留 pending。
func TestCreateEnvCanceledKeepsIntentPending(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	entered := make(chan struct{})
	f.prov.beforeCreate = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.c.CreateEnv(ctx, req("e1"))
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
	if s := f.store.intentState("e1"); s != IntentPending {
		t.Fatalf("intent = %s, want pending", s)
	}
}

// TestCreateEnvFailureWithoutResidueFailsIntent：失败且独立扫描确认无残留时 intent 置为 failed。
func TestCreateEnvFailureWithoutResidueFailsIntent(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	r := req("e1")
	r.Template = "" // provider 校验失败，未创建任何资源
	if _, err := f.c.CreateEnv(context.Background(), r); err == nil {
		t.Fatal("CreateEnv 应失败")
	}
	if s := f.store.intentState("e1"); s != IntentFailed {
		t.Fatalf("intent = %s, want failed", s)
	}
}

// TestCreateEnvAfterStopRefused：已确认停止的环境不再创建，不触碰 provider。
func TestCreateEnvAfterStopRefused(t *testing.T) {
	f := newFixture(t, true)
	f.createStopped(t, "e1", false)
	if _, err := f.c.CreateEnv(context.Background(), req("e1")); !errors.Is(err, ErrEnvStopped) {
		t.Fatalf("err = %v, want ErrEnvStopped", err)
	}
	assertLog(t, f.rec)
}

// ---- StopEnv ----

// TestStopEnvUnconfirmedBlocks：ErrStopUnconfirmed → Blocked，不写 stopped_at。
func TestStopEnvUnconfirmedBlocks(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	if _, err := f.c.CreateEnv(context.Background(), req("e1")); err != nil {
		t.Fatal(err)
	}
	f.prov.FailStop("e1", provider.ErrStopUnconfirmed)
	res, err := f.c.StopEnv(context.Background(), "e1")
	if err != nil || !res.Blocked || res.Stopped || res.Recorded {
		t.Fatalf("StopEnv = %+v, %v", res, err)
	}
	if f.rec.count("MarkStopped") != 0 || f.store.env("e1").StoppedAt != nil {
		t.Fatal("未确认停止时写了 stopped_at")
	}
}

// TestStopEnvStoreFailureCompensates：Store 写失败时以退避重试提交；ctx 结束后保留"已停止"
// 事实，下次调用以原时间补提交且不再调用 provider（规格 §14.5）。
func TestStopEnvStoreFailureCompensates(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	f.store.addEnv("e2", false)
	ctx := context.Background()
	for _, id := range []string{"e1", "e2"} {
		if _, err := f.c.CreateEnv(ctx, req(id)); err != nil {
			t.Fatal(err)
		}
	}

	// 暂时性失败：同一次调用内重试成功。
	f.store.failNext("MarkStopped", 2)
	res, err := f.c.StopEnv(ctx, "e1")
	if err != nil || !res.Stopped || !res.Recorded || f.store.env("e1").StoppedAt == nil {
		t.Fatalf("StopEnv = %+v, %v", res, err)
	}

	// 持续失败直到 ctx 结束：停止本身已确认，不是错误；报告 Recorded=false；恢复后补提交原时间。
	f.store.failNext("MarkStopped", 1000)
	cctx, cancel := context.WithCancel(ctx)
	fails := 0
	f.store.onFail = func(string) {
		if fails++; fails == 3 {
			cancel()
		}
	}
	res, err = f.c.StopEnv(cctx, "e2")
	if err != nil || !res.Stopped || res.Recorded || f.store.env("e2").StoppedAt != nil {
		t.Fatalf("StopEnv = %+v, %v；want 已停止且未提交", res, err)
	}
	at := res.At
	if _, err := f.c.CreateEnv(ctx, req("e2")); !errors.Is(err, ErrEnvStopped) {
		t.Fatalf("待提交的停止事实之后 CreateEnv = %v, want ErrEnvStopped", err)
	}
	f.store.failNext("MarkStopped", 0)
	f.clock.advance(time.Minute)
	res, err = f.c.StopEnv(ctx, "e2")
	if err != nil || !res.Stopped || !res.Recorded || !res.At.Equal(at) {
		t.Fatalf("补提交 StopEnv = %+v, %v；want At = %v", res, err, at)
	}
	if got := f.store.env("e2").StoppedAt; got == nil || !got.Equal(at) {
		t.Fatalf("stopped_at = %v, want %v", got, at)
	}
	if n := f.rec.count("Stop:ok"); n != 2 { // e1、e2 各一次
		t.Fatalf("provider.Stop 调用 %d 次, want 2", n)
	}
}

// TestSameEnvSerialized：同一 env 的 CreateEnv 与 StopEnv 串行；其他 env 不受阻塞。
func TestSameEnvSerialized(t *testing.T) {
	f := newFixture(t, true)
	f.store.addEnv("e1", false)
	f.store.addEnv("e2", false)
	entered, release := make(chan struct{}), make(chan struct{})
	f.prov.beforeCreate = func(ctx context.Context) error {
		close(entered)
		<-release
		return nil
	}
	waiting := make(chan string, 1)
	f.c.onLockWait = func(id string) { waiting <- id }

	ctx := context.Background()
	createDone := make(chan error, 1)
	go func() {
		_, err := f.c.CreateEnv(ctx, req("e1"))
		createDone <- err
	}()
	<-entered
	stopDone := make(chan error, 1)
	go func() {
		res, err := f.c.StopEnv(ctx, "e1")
		if err == nil && !res.Stopped {
			err = fmt.Errorf("StopEnv = %+v", res)
		}
		stopDone <- err
	}()
	if id := <-waiting; id != "e1" {
		t.Fatalf("等待的 env = %s", id)
	}
	// 其他环境的停止不等待 e1 的创建。
	if res, err := f.c.StopEnv(ctx, "e2"); err != nil || !res.Stopped {
		t.Fatalf("StopEnv(e2) = %+v, %v", res, err)
	}
	if n := f.rec.count("Stop:ok"); n != 1 { // 只有 e2 的停止
		t.Fatalf("e1 的停止在创建返回前执行: %v", f.rec.snapshot())
	}
	close(release)
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	log := f.rec.snapshot()
	created := slices.Index(log, "ResolveIntent:acquired")
	stops := 0
	for i, s := range log {
		if s == "Stop:ok" {
			if stops++; stops == 2 && i < created { // 第 1 次是 e2，第 2 次是 e1
				t.Fatalf("e1 的停止早于创建完成: %v", log)
			}
		}
	}
	if stops != 2 {
		t.Fatalf("调用记录 %v", log)
	}
}

// ---- cleanup ----

// TestCleanupReleasesUIDRange：Destroy → intent released → done → 归还 UID 范围。
func TestCleanupReleasesUIDRange(t *testing.T) {
	f := newFixture(t, true)
	f.createStopped(t, "e1", true)
	if err := f.c.cleanupPass(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertLog(t, f.rec, "Destroy:ok", "ResolveIntent:released", "UpdateCleanup:done", "ReleaseUIDRange")
	if r := f.store.rangeOf(fmt.Sprintf("uid-%d", testUIDBase)); r.State != "free" {
		t.Fatalf("UID 范围 = %+v", r)
	}
	if e := f.store.env("e1"); e.CleanupState != CleanupDone || e.StoppedAt == nil {
		t.Fatalf("env = %+v", e)
	}
}

// TestCleanupBacksOffOnFailure：Destroy 失败 → pending + error + next_retry_at；退避未到不重试，
// 到期后重试成功。
func TestCleanupBacksOffOnFailure(t *testing.T) {
	f := newFixture(t, false)
	f.createStopped(t, "e1", true)
	f.prov.destroyErrs = []error{errors.New("EBUSY")}
	ctx := context.Background()
	t0 := f.clock.now()
	if err := f.c.cleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	e := f.store.env("e1")
	if e.CleanupState != CleanupPending || e.CleanupTries != 1 || !strings.Contains(e.CleanupError, "EBUSY") ||
		e.NextRetryAt == nil || !e.NextRetryAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("env = %+v", e)
	}
	f.clock.advance(500 * time.Millisecond)
	if err := f.c.cleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	if n := f.rec.count("Destroy:ok") + f.rec.count("Destroy:err"); n != 1 {
		t.Fatalf("退避期内重试了 Destroy（%d 次）", n)
	}
	f.clock.advance(time.Second)
	if err := f.c.cleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	if e := f.store.env("e1"); e.CleanupState != CleanupDone || e.CleanupTries != 2 {
		t.Fatalf("env = %+v", e)
	}
	if r := f.store.rangeOf(fmt.Sprintf("uid-%d", testUIDBase)); r.State != "free" {
		t.Fatalf("UID 范围 = %+v", r)
	}
}

// TestCleanupSkipsUnstopped：未停止的环境（以及 attempt 尚无判决的）不处理。
func TestCleanupSkipsUnstopped(t *testing.T) {
	f := newFixture(t, true)
	ctx := context.Background()
	f.store.addEnv("running", true)
	if _, err := f.c.CreateEnv(ctx, req("running")); err != nil {
		t.Fatal(err)
	}
	f.prov.FailStop("running", provider.ErrStopUnconfirmed)
	if res, _ := f.c.StopEnv(ctx, "running"); !res.Blocked {
		t.Fatalf("StopEnv = %+v", res)
	}
	f.createStopped(t, "undecided", false)
	if err := f.c.cleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	assertLog(t, f.rec)
	for _, id := range []string{"running", "undecided"} {
		if e := f.store.env(id); e.CleanupState != CleanupNone {
			t.Fatalf("%s 被清理: %+v", id, e)
		}
	}
}

// TestCleanupRetriesFailedRelease：done 之后归还失败，环境不再是候选；后续轮次补做归还。
func TestCleanupRetriesFailedRelease(t *testing.T) {
	f := newFixture(t, true)
	f.createStopped(t, "e1", true)
	f.store.failNext("ReleaseUIDRange", 1)
	ctx := context.Background()
	if err := f.c.cleanupPass(ctx); !errors.Is(err, persistence.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	id := fmt.Sprintf("uid-%d", testUIDBase)
	if r := f.store.rangeOf(id); r.State != "assigned" {
		t.Fatalf("UID 范围 = %+v", r)
	}
	if err := f.c.cleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	if r := f.store.rangeOf(id); r.State != "free" {
		t.Fatalf("UID 范围未补做归还: %+v", r)
	}
	if n := f.rec.count("Destroy:ok"); n != 1 {
		t.Fatalf("Destroy 调用 %d 次, want 1", n)
	}
}

// TestRunCleanupLoop：循环执行清理并在 ctx 结束时返回。
func TestRunCleanupLoop(t *testing.T) {
	f := newFixture(t, true)
	f.createStopped(t, "e1", true)
	ctx, cancel := context.WithCancel(context.Background())
	pass := make(chan struct{}, 1)
	f.c.mu.Lock()
	f.c.onCleanupRun = func() {
		select {
		case pass <- struct{}{}:
		default:
		}
	}
	f.c.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- f.c.RunCleanup(ctx) }()
	<-pass
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCleanup = %v", err)
	}
	if e := f.store.env("e1"); e.CleanupState != CleanupDone {
		t.Fatalf("env = %+v", e)
	}
}
