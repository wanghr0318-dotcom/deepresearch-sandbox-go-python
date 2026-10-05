package ownership

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

var (
	tokA = bytes.Repeat([]byte{0xA}, TokenSize)
	tokB = bytes.Repeat([]byte{0xB}, TokenSize)
)

// TestDecideCoversSpecTable 逐行对应规格 §7.4 的决策表（含引导令牌修订）。
func TestDecideCoversSpecTable(t *testing.T) {
	hashA := tokenHash(tokA)
	empty := DBState{}
	noRecord := DBState{HasMigrations: true, HasAgentboxTables: true}
	unknown := DBState{HasAgentboxTables: true}
	pending := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a", TokenHash: hashA}}
	legacy := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a"}}
	done := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a", Complete: true, TokenHash: hashA}}
	none, same, other := FileState{}, FileState{Exists: true, InstallID: "a"}, FileState{Exists: true, InstallID: "b"}
	tokenOnly := FileState{TokenHash: hashA}
	otherToken := FileState{TokenHash: tokenHash(tokB)}

	cases := []struct {
		name string
		db   DBState
		file FileState
		want Action
	}{
		{"空库，无身份文件：全新安装", empty, none, Initialize},
		{"空库，无身份文件但有令牌：全新安装（复用令牌）", empty, tokenOnly, Initialize},
		{"有 schema_migrations 无记录", noRecord, none, Refuse},
		{"有 schema_migrations 无记录（有文件）", noRecord, same, Refuse},
		{"空库但有身份文件", empty, same, Refuse},
		{"未知 schema", unknown, none, Refuse},
		{"未知 schema（有文件）", unknown, same, Refuse},
		{"pending，无身份文件，令牌一致：继续引导", pending, tokenOnly, WriteFileAndComplete},
		{"pending，无身份文件，无令牌：拒绝", pending, none, Refuse},
		{"pending，无身份文件，令牌不同：拒绝（另一个数据目录）", pending, otherToken, Refuse},
		{"旧 pending（无哈希），无身份文件：拒绝", legacy, tokenOnly, Refuse},
		{"旧 pending（无哈希），身份一致：置 complete", legacy, same, Complete},
		{"pending，身份一致", pending, same, Complete},
		{"pending，身份不一致", pending, other, Refuse},
		{"complete，身份不一致", done, other, Refuse},
		{"complete，无身份文件", done, none, Refuse},
		{"complete，无身份文件但令牌一致", done, tokenOnly, Refuse},
		{"complete，身份一致", done, same, Proceed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Decide(c.db, c.file)
			if d.Action != c.want {
				t.Fatalf("得到 %v（%s），期望 %v", d.Action, d.Reason, c.want)
			}
			if (d.Action == Refuse) != (d.Reason != "") {
				t.Fatalf("只有拒绝时才有原因：%+v", d)
			}
		})
	}
}

type fakeStore struct {
	db        DBState
	failAfter string    // 在该步骤成功后模拟进程中断
	tokens    *fakeFile // 若设置，InitializeInstallation 断言令牌已先持久化
}

var errCrash = errors.New("模拟中断")

func (s *fakeStore) InspectInstallation(context.Context) (DBState, error) { return s.db, nil }

func (s *fakeStore) InitializeInstallation(_ context.Context, id string, hash []byte) error {
	if s.tokens != nil && (!s.tokens.exists || !bytes.Equal(tokenHash(s.tokens.token), hash)) {
		return errors.New("初始化事务之前令牌必须已持久化，且哈希与之一致")
	}
	s.db = DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: id, TokenHash: hash}}
	if s.failAfter == "initialize" {
		return errCrash
	}
	return nil
}

func (s *fakeStore) CompleteInstallation(_ context.Context, id string) error {
	if s.db.Installation == nil || s.db.Installation.InstallID != id {
		return errors.New("CompleteInstallation 的 install_id 与 pending 记录不一致")
	}
	s.db.Installation.Complete = true
	return nil
}

// fakeFile 同时充当 IDFile 与 TokenFile（各用一个实例）。
type fakeFile struct {
	id        string
	token     []byte
	exists    bool
	failAfter bool  // 写入成功后模拟中断
	readErr   error // 读取失败或损坏
	writes    int
}

func (f *fakeFile) Read() (string, bool, error) { return f.id, f.exists, f.readErr }

func (f *fakeFile) Write(id string) error {
	f.id, f.exists = id, true
	f.writes++
	if f.failAfter {
		return errCrash
	}
	return nil
}

type fakeToken struct{ *fakeFile }

func (f fakeToken) Read() ([]byte, bool, error) { return f.token, f.exists, f.readErr }

func (f fakeToken) Write(token []byte) error {
	if f.exists {
		return errors.New("令牌已存在，不能覆盖")
	}
	f.token, f.exists = token, true
	f.writes++
	return nil
}

func newTok(tok []byte) func() ([]byte, error) { return func() ([]byte, error) { return tok, nil } }

// TestBootstrapResumesAfterEachInterruption 覆盖 E46 中初始化事务提交之后的两个中断点：每次中断后
// 重新引导都能完成（令牌一致），且中断时不返回 install_id。提交前中断与提交结果未知由 postgres 包的
// TestInstallationBootstrapE46 在真实事务上覆盖。
func TestBootstrapResumesAfterEachInterruption(t *testing.T) {
	for _, point := range []string{"initialize", "write"} {
		t.Run(point, func(t *testing.T) {
			tokens := &fakeFile{}
			store := &fakeStore{failAfter: point, tokens: tokens}
			file := &fakeFile{failAfter: point == "write"}
			if id, err := Bootstrap(context.Background(), store, file, fakeToken{tokens}, func() string { return "new" }, newTok(tokA)); !errors.Is(err, errCrash) || id != "" {
				t.Fatalf("第一次引导应中断且不返回 install_id，得到 (%q, %v)", id, err)
			}
			store.failAfter, file.failAfter = "", false
			id, err := Bootstrap(context.Background(), store, file, fakeToken{tokens},
				func() string { t.Fatal("不应再生成新 ID"); return "" }, func() ([]byte, error) { t.Fatal("不应再生成令牌"); return nil, nil })
			if err != nil || id != "new" || !store.db.Installation.Complete || file.id != "new" {
				t.Fatalf("重新引导得到 (%q, %v)，状态 %+v 文件 %+v", id, err, store.db.Installation, file)
			}
			if tokens.writes != 1 {
				t.Fatalf("令牌应只写入一次，得到 %d 次", tokens.writes)
			}
		})
	}
}

// TestBootstrapReusesExistingToken：令牌已持久、初始化事务未提交时崩溃，重启复用同一令牌。
func TestBootstrapReusesExistingToken(t *testing.T) {
	tokens := &fakeFile{token: tokA, exists: true}
	store := &fakeStore{tokens: tokens}
	if _, err := Bootstrap(context.Background(), store, &fakeFile{}, fakeToken{tokens}, func() string { return "new" },
		func() ([]byte, error) { t.Fatal("已有令牌时不应生成新令牌"); return nil, nil }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(store.db.Installation.TokenHash, tokenHash(tokA)) || tokens.writes != 0 {
		t.Fatalf("应复用已有令牌：哈希 %x，写入 %d 次", store.db.Installation.TokenHash, tokens.writes)
	}
}

// TestBootstrapTokenFailuresStopStartup：令牌读取失败或损坏、生成的令牌长度不对、数据库中的哈希
// 长度不对——都在初始化之前失败，不生成新身份。
func TestBootstrapTokenFailuresStopStartup(t *testing.T) {
	cases := map[string]struct {
		store  *fakeStore
		tokens *fakeFile
		gen    func() ([]byte, error)
	}{
		"令牌读取失败":      {&fakeStore{}, &fakeFile{readErr: errors.New("EIO")}, newTok(tokA)},
		"令牌损坏":        {&fakeStore{}, &fakeFile{readErr: errors.New("损坏")}, newTok(tokA)},
		"生成的令牌长度不对":   {&fakeStore{}, &fakeFile{}, newTok(tokA[:16])},
		"数据库令牌哈希长度不对": {&fakeStore{db: DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a", TokenHash: []byte{1, 2, 3}}}}, &fakeFile{token: tokA, exists: true}, newTok(tokA)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			before := c.store.db
			id, err := Bootstrap(context.Background(), c.store, &fakeFile{}, fakeToken{c.tokens}, func() string { return "new" }, c.gen)
			if err == nil || id != "" {
				t.Fatalf("应失败而不是继续引导，得到 (%q, %v)", id, err)
			}
			if c.store.db.Installation != before.Installation {
				t.Fatal("失败时不应初始化数据库")
			}
		})
	}
}

// TestBootstrapRefusesOtherDataDirectory：pending 记录由数据目录 A 发起，数据目录 B（不同令牌或无令牌）被拒绝。
func TestBootstrapRefusesOtherDataDirectory(t *testing.T) {
	for name, tokens := range map[string]*fakeFile{"不同令牌": {token: tokB, exists: true}, "无令牌": {}} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{db: DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a", TokenHash: tokenHash(tokA)}}}
			file := &fakeFile{}
			_, err := Bootstrap(context.Background(), store, file, fakeToken{tokens}, func() string { return "x" }, newTok(tokB))
			if !errors.Is(err, ErrRefused) || store.db.Installation.Complete || file.exists || tokens.writes != 0 {
				t.Fatalf("应拒绝且不改变任何状态：err %v，记录 %+v，身份文件 %v，令牌写入 %d", err, store.db.Installation, file.exists, tokens.writes)
			}
		})
	}
}

func TestBootstrapRejectsEmptyNewID(t *testing.T) {
	store := &fakeStore{}
	if id, err := Bootstrap(context.Background(), store, &fakeFile{}, fakeToken{&fakeFile{}}, func() string { return "" }, newTok(tokA)); err == nil || id != "" {
		t.Fatalf("空的 install_id 应被拒绝，得到 (%q, %v)", id, err)
	}
	if store.db.Installation != nil {
		t.Fatal("拒绝空 install_id 时不应初始化数据库")
	}
}

func TestBootstrapRefusesWithReason(t *testing.T) {
	store := &fakeStore{db: DBState{HasMigrations: true}}
	_, err := Bootstrap(context.Background(), store, &fakeFile{}, fakeToken{&fakeFile{}}, func() string { return "x" }, newTok(tokA))
	var refused *RefusedError
	if !errors.As(err, &refused) || !errors.Is(err, ErrRefused) || refused.Reason == "" {
		t.Fatalf("应拒绝并给出原因，得到 %v", err)
	}
}
