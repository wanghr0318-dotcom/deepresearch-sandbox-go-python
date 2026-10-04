package ownership

import (
	"context"
	"errors"
	"testing"
)

// TestDecideCoversSpecTable 逐行对应规格 §7.4 的决策表。
func TestDecideCoversSpecTable(t *testing.T) {
	empty := DBState{}
	noRecord := DBState{HasMigrations: true, HasAgentboxTables: true}
	unknown := DBState{HasAgentboxTables: true}
	pending := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a"}}
	done := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a", Complete: true}}
	none, same, other := FileState{}, FileState{Exists: true, InstallID: "a"}, FileState{Exists: true, InstallID: "b"}

	cases := []struct {
		name string
		db   DBState
		file FileState
		want Action
	}{
		{"空库，无身份文件：全新安装", empty, none, Initialize},
		{"有 schema_migrations 无记录", noRecord, none, Refuse},
		{"有 schema_migrations 无记录（有文件）", noRecord, same, Refuse},
		{"空库但有身份文件", empty, same, Refuse},
		{"未知 schema", unknown, none, Refuse},
		{"未知 schema（有文件）", unknown, same, Refuse},
		{"pending，无身份文件：继续引导", pending, none, WriteFileAndComplete},
		{"pending，身份一致", pending, same, Complete},
		{"pending，身份不一致", pending, other, Refuse},
		{"complete，身份不一致", done, other, Refuse},
		{"complete，无身份文件", done, none, Refuse},
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
	failAfter string // 在该步骤成功后模拟进程中断
}

var errCrash = errors.New("模拟中断")

func (s *fakeStore) InspectInstallation(context.Context) (DBState, error) { return s.db, nil }

func (s *fakeStore) InitializeInstallation(_ context.Context, id string) error {
	s.db = DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: id}}
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

type fakeFile struct {
	id        string
	exists    bool
	failAfter bool
}

func (f *fakeFile) Read() (string, bool, error) { return f.id, f.exists, nil }

func (f *fakeFile) Write(id string) error {
	f.id, f.exists = id, true
	if f.failAfter {
		return errCrash
	}
	return nil
}

// TestBootstrapResumesAfterEachInterruption 覆盖 E46 中初始迁移提交之后的两个中断点：每次中断后
// 重新引导都能完成，且中断时不返回 install_id。提交前中断由 postgres 包的 TestInstallationBootstrapE46
// 在真实事务上覆盖。
func TestBootstrapResumesAfterEachInterruption(t *testing.T) {
	for _, point := range []string{"initialize", "write"} {
		t.Run(point, func(t *testing.T) {
			store := &fakeStore{failAfter: point}
			file := &fakeFile{failAfter: point == "write"}
			if id, err := Bootstrap(context.Background(), store, file, func() string { return "new" }); !errors.Is(err, errCrash) || id != "" {
				t.Fatalf("第一次引导应中断且不返回 install_id，得到 (%q, %v)", id, err)
			}
			store.failAfter, file.failAfter = "", false
			id, err := Bootstrap(context.Background(), store, file, func() string { t.Fatal("不应再生成新 ID"); return "" })
			if err != nil || id != "new" || !store.db.Installation.Complete || file.id != "new" {
				t.Fatalf("重新引导得到 (%q, %v)，状态 %+v 文件 %+v", id, err, store.db.Installation, file)
			}
		})
	}
}

func TestBootstrapRejectsEmptyNewID(t *testing.T) {
	store := &fakeStore{}
	if id, err := Bootstrap(context.Background(), store, &fakeFile{}, func() string { return "" }); err == nil || id != "" {
		t.Fatalf("空的 install_id 应被拒绝，得到 (%q, %v)", id, err)
	}
	if store.db.Installation != nil {
		t.Fatal("拒绝空 install_id 时不应初始化数据库")
	}
}

func TestBootstrapRefusesWithReason(t *testing.T) {
	store := &fakeStore{db: DBState{HasMigrations: true}}
	_, err := Bootstrap(context.Background(), store, &fakeFile{}, func() string { return "x" })
	var refused *RefusedError
	if !errors.As(err, &refused) || !errors.Is(err, ErrRefused) || refused.Reason == "" {
		t.Fatalf("应拒绝并给出原因，得到 %v", err)
	}
}
