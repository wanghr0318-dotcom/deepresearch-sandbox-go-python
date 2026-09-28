package main

import (
	"fmt"
	"os"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/hostcheck"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "doctor" {
		r := hostcheck.Check()
		if err := r.Err(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("宿主环境检查通过")
		return
	}
	fmt.Fprintln(os.Stderr, "用法: agentbox doctor")
	os.Exit(2)
}
