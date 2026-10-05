package main

import (
	"strings"
	"testing"
)

func judge(t *testing.T, events string, allowed string) Verdict {
	t.Helper()
	a, err := ParseAllowed(strings.NewReader(allowed))
	if err != nil {
		t.Fatalf("ParseAllowed: %v", err)
	}
	v, err := Judge(strings.NewReader(events), a)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	return v
}

func TestAllPassedIsOK(t *testing.T) {
	v := judge(t, `{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"pass","Package":"p"}`, "")
	if !v.OK() || v.Passed != 1 {
		t.Fatalf("应通过，得到 %+v", v)
	}
}

func TestUnexpectedSkipFails(t *testing.T) {
	v := judge(t, `{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"skip","Package":"p","Test":"TestB"}`, "# 无允许项\n")
	if v.OK() || len(v.UnexpectedSkips) != 1 || v.UnexpectedSkips[0] != "p TestB" {
		t.Fatalf("非预期 skip 应失败，得到 %+v", v)
	}
}

func TestAllowedSkipPasses(t *testing.T) {
	v := judge(t, `{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"skip","Package":"p","Test":"TestB"}
{"Action":"pass","Package":"p"}`, "# 原因：仅非 root 有意义\np TestB\n")
	if !v.OK() {
		t.Fatalf("允许清单中的 skip 应通过，得到 %+v", v)
	}
}

func TestPackageWithoutTestsIsNotASkip(t *testing.T) {
	v := judge(t, `{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"pass","Package":"p"}
{"Action":"skip","Package":"q"}`, "")
	if !v.OK() {
		t.Fatalf("无测试文件的包不应计为 skip，得到 %+v", v)
	}
}

func TestFailedTestAndPackageFail(t *testing.T) {
	v := judge(t, `{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"fail","Package":"p","Test":"TestC"}
{"Action":"fail","Package":"r"}`, "")
	if v.OK() || len(v.FailedTests) != 1 || len(v.FailedPackages) != 1 {
		t.Fatalf("失败应被识别，得到 %+v", v)
	}
}

func TestNothingPassedFails(t *testing.T) {
	v := judge(t, `{"Action":"skip","Package":"q"}`, "")
	if v.OK() {
		t.Fatalf("没有任何用例通过应失败，得到 %+v", v)
	}
}

// 部分用例通过后测试进程被杀：已开始的用例没有结束事件，包也没有结束事件。
// 只看"有通过、无失败"会误判为成功。
func TestInterruptedAfterPartialPassFails(t *testing.T) {
	v := judge(t, `{"Action":"start","Package":"p"}
{"Action":"run","Package":"p","Test":"TestA"}
{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"run","Package":"p","Test":"TestB"}`, "")
	if v.OK() {
		t.Fatalf("中断的测试流应判失败，得到 %+v", v)
	}
	if len(v.IncompleteTests) != 1 || v.IncompleteTests[0] != "p TestB" {
		t.Fatalf("应报告未结束的用例 p TestB，得到 %+v", v.IncompleteTests)
	}
	if len(v.IncompletePackages) != 1 || v.IncompletePackages[0] != "p" {
		t.Fatalf("应报告未结束的包 p，得到 %+v", v.IncompletePackages)
	}
}

func TestCompleteStreamWithPackageEndIsOK(t *testing.T) {
	v := judge(t, `{"Action":"start","Package":"p"}
{"Action":"run","Package":"p","Test":"TestA"}
{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"pass","Package":"p"}`, "")
	if !v.OK() {
		t.Fatalf("完整的测试流应通过，得到 %+v", v)
	}
}

func TestMalformedInputIsError(t *testing.T) {
	a, _ := ParseAllowed(strings.NewReader(""))
	if _, err := Judge(strings.NewReader("{not json"), a); err == nil {
		t.Fatal("非法输入应返回错误")
	}
}
