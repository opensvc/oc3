package feederhandlers

import "testing"

// The wake a peer dequeues names its target, as the server queues it; another
// command, or a wake the server never queues, is not one to cancel.
func TestWakeCommandPattern(t *testing.T) {
	for command, want := range map[string]string{
		"wake --node w3":             "w3",
		"wake --node n2.lab.example": "n2.lab.example",
		"wake --node w3 --wait":      "",
		"wake --node":                "",
		"wake --local --node=w3":     "",
		"svc -s foo wake --node w3":  "",
		"reboot":                     "",
	} {
		got := ""
		if m := wakeCommandPattern.FindStringSubmatch(command); m != nil {
			got = m[1]
		}
		if got != want {
			t.Errorf("%q: got target %q, want %q", command, got, want)
		}
	}
}
