package serverhandlers

import (
	"errors"
	"net/http"
	"testing"
)

func TestWakeCommand(t *testing.T) {
	for _, name := range []string{"k2", "dev4n2", "n2.lab.example", "node_2"} {
		got, err := wakeCommand(name)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		if want := "wake --node " + name; got != want {
			t.Fatalf("%s: got %q, want %q", name, got, want)
		}
	}
	// A name the agent would split in several words, or read as a flag, is
	// refused rather than queued.
	for _, name := range []string{"", "k2 --mac 00:11:22:33:44:55", "--local", "k2;reboot", "-k2"} {
		_, err := wakeCommand(name)
		var refusal *actionRefusal
		if !errors.As(err, &refusal) || refusal.status != http.StatusBadRequest {
			t.Fatalf("%q: want a 400 refusal, got %v", name, err)
		}
	}
}
