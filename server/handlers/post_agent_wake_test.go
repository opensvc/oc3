package serverhandlers

import (
	"errors"
	"net/http"
	"testing"

	"github.com/opensvc/oc3/cdb"
)

func TestPickWakeContainer(t *testing.T) {
	const (
		target = "k2-node-id"
		hyper  = "hyper-node-id"
		kvm    = "kvm-svc-id"
	)
	c2 := cdb.WakeContainer{NodeID: hyper, SvcID: kvm, RID: "container#2"}
	for _, tc := range []struct {
		name       string
		containers []cdb.WakeContainer
		want       cdb.WakeContainer
		wantStatus int
	}{
		{name: "the one container", containers: []cdb.WakeContainer{c2}, want: c2},
		{name: "a row of the node itself is not a container running it",
			containers: []cdb.WakeContainer{{NodeID: target, SvcID: "x", RID: "container#1"}, c2}, want: c2},
		{name: "a row without rid is not a container", containers: []cdb.WakeContainer{{NodeID: hyper, SvcID: kvm}, c2}, want: c2},
		{name: "none", wantStatus: http.StatusNotFound},
		{name: "only the node itself", containers: []cdb.WakeContainer{{NodeID: target, SvcID: "x", RID: "container#1"}}, wantStatus: http.StatusNotFound},
		{name: "several, as a virtual machine declared twice",
			containers: []cdb.WakeContainer{c2, {NodeID: "other-hv", SvcID: "other-svc", RID: "container#1"}}, wantStatus: http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pickWakeContainer(tc.containers, target, "k2", "c5f98577-2918-4da7-b7c9-0ae24687a572")
			if tc.wantStatus != 0 {
				var refusal *actionRefusal
				if !errors.As(err, &refusal) || refusal.status != tc.wantStatus {
					t.Fatalf("want a %d refusal, got %v", tc.wantStatus, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
