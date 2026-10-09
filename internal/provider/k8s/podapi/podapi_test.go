package podapi

import (
	"reflect"
	"testing"
)

func TestParseAck(t *testing.T) {
	cases := []struct {
		line    string
		started bool
		pid     int
		reason  string
		bad     bool
	}{
		{"ABX-STARTED 17", true, 17, "", false},
		{"ABX-START-ERR python3: not found", false, 0, "python3: not found", false},
		{"ABX-STARTED x", false, 0, "", true},
		{"{\"type\":\"ready\"}", false, 0, "", true},
	}
	for _, c := range cases {
		started, pid, reason, err := ParseAck(c.line)
		if started != c.started || pid != c.pid || reason != c.reason || (err != nil) != c.bad {
			t.Errorf("ParseAck(%q) = %v %d %q %v", c.line, started, pid, reason, err)
		}
	}
}

func TestExecArgv(t *testing.T) {
	got := ExecArgv("a1", []string{"A=1"}, "/workspace", 1024, 0, []string{"python3", "-m", "w"})
	want := []string{Binary, "exec", "--id", "a1", "--env", "A=1", "--dir", "/workspace", "--nofile", "1024", "--",
		"python3", "-m", "w"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q", got)
	}
}
