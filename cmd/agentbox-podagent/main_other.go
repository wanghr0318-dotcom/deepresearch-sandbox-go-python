//go:build !linux

// Command agentbox-podagent runs only inside Linux sandbox Pods; on other platforms it only builds.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "agentbox-podagent: Linux only")
	os.Exit(2)
}
