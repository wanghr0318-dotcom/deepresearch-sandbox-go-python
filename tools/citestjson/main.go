// Command citestjson 判定 `go test -json` 的结果，供 CI 的 Linux 集成测试使用。
//
// 它只是 go test 退出码之外的补充检查，不能替代退出码：CI 必须同时要求
// go test 退出码为 0 与本工具判定通过。
//
// 判定规则：
//   - 任何失败的用例或失败的包 → 失败；
//   - 任何被跳过的用例 → 失败，除非列在允许清单中；
//   - 已开始但没有结束事件的用例，或没有结束事件的包 → 失败（测试进程被中断）；
//   - 没有任何用例通过 → 失败（防止测试实际上没有运行）。
//
// 用法：citestjson <test.json> <allowed-skips.txt>
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type event struct {
	Action  string
	Package string
	Test    string
}

// Verdict 是一次判定的结果。
type Verdict struct {
	FailedTests        []string
	FailedPackages     []string
	UnexpectedSkips    []string
	IncompleteTests    []string
	IncompletePackages []string
	Passed             int
}

// OK 报告判定是否通过。
func (v Verdict) OK() bool {
	return len(v.FailedTests) == 0 && len(v.FailedPackages) == 0 &&
		len(v.UnexpectedSkips) == 0 && len(v.IncompleteTests) == 0 &&
		len(v.IncompletePackages) == 0 && v.Passed > 0
}

// Judge 读取 go test -json 输出并依据允许清单作出判定。
func Judge(testJSON io.Reader, allowed map[string]bool) (Verdict, error) {
	var v Verdict
	runningTests := map[string]bool{}
	openPackages := map[string]bool{}

	dec := json.NewDecoder(testJSON)
	for {
		var e event
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return v, fmt.Errorf("解析 go test -json 输出: %w", err)
		}
		id := e.Package + " " + e.Test
		ends := e.Action == "pass" || e.Action == "fail" || e.Action == "skip"

		if e.Test == "" {
			if ends {
				delete(openPackages, e.Package)
			} else {
				openPackages[e.Package] = true
			}
		} else {
			openPackages[e.Package] = true
			if e.Action == "run" {
				runningTests[id] = true
			} else if ends {
				delete(runningTests, id)
			}
		}

		switch {
		case e.Action == "fail" && e.Test != "":
			v.FailedTests = append(v.FailedTests, id)
		case e.Action == "fail":
			v.FailedPackages = append(v.FailedPackages, e.Package)
		case e.Action == "skip" && e.Test != "" && !allowed[id]:
			v.UnexpectedSkips = append(v.UnexpectedSkips, id)
		case e.Action == "pass" && e.Test != "":
			v.Passed++
		}
	}

	v.IncompleteTests = sortedKeys(runningTests)
	v.IncompletePackages = sortedKeys(openPackages)
	return v, nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ParseAllowed 读取允许清单：每行 "<包路径> <用例名>"，忽略空行与 # 注释。
func ParseAllowed(r io.Reader) (map[string]bool, error) {
	allowed := map[string]bool{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		allowed[line] = true
	}
	return allowed, sc.Err()
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "用法: citestjson <test.json> <allowed-skips.txt>")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(jsonPath, allowPath string) error {
	af, err := os.Open(allowPath)
	if err != nil {
		return err
	}
	defer af.Close()
	allowed, err := ParseAllowed(af)
	if err != nil {
		return err
	}

	jf, err := os.Open(jsonPath)
	if err != nil {
		return err
	}
	defer jf.Close()
	v, err := Judge(jf, allowed)
	if err != nil {
		return err
	}

	report("失败的用例", v.FailedTests)
	report("失败的包", v.FailedPackages)
	report("非预期的 skip（必需用例因环境缺失而跳过，视为失败）", v.UnexpectedSkips)
	report("已开始但未结束的用例（测试进程可能被中断）", v.IncompleteTests)
	report("没有结束事件的包（测试进程可能被中断）", v.IncompletePackages)
	fmt.Printf("通过的用例数：%d\n", v.Passed)
	if v.Passed == 0 {
		fmt.Println("没有任何用例通过：测试可能没有实际运行")
	}
	if !v.OK() {
		return fmt.Errorf("Linux 集成测试判定未通过")
	}
	return nil
}

func report(title string, items []string) {
	if len(items) == 0 {
		return
	}
	fmt.Println(title + "：")
	for _, it := range items {
		fmt.Println("  " + it)
	}
}
