package redcache

import (
	"bytes"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
)

// TestMain fails the run if any goroutine running package code is leaked:
// blocked on a primitive no runnable goroutine can reach (Go 1.27's
// goroutineleak profile). Stacks rooted in test code are ignored.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if leaks := leakedGoroutines(); leaks != "" {
			fmt.Fprintf(os.Stderr, "leaked goroutines:\n%s\n", leaks)
			code = 1
		}
	}
	os.Exit(code)
}

func leakedGoroutines() string {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutineleak").WriteTo(&buf, 1); err != nil {
		return err.Error()
	}
	var leaks []string
	for stack := range strings.SplitSeq(buf.String(), "\n\n") {
		if strings.Contains(stack, "github.com/dcbickfo/redcache.") && !strings.Contains(stack, "_test.go") {
			leaks = append(leaks, stack)
		}
	}
	return strings.Join(leaks, "\n\n")
}
