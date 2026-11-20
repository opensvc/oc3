package serverhandlers

import (
	"net/http"
	"testing"
	"time"
)

// TestDeleteNodeDenied checks a caller without NodeManager is stopped before
// the database: the Api has none, a handler going on with the deletion panics.
func TestDeleteNodeDenied(t *testing.T) {
	a := &Api{SyncTimeout: time.Second}
	for _, tc := range []struct {
		what string
		call func(a *Api, groups ...string) (int, string)
	}{
		{"DELETE /nodes/{node_id}", func(a *Api, groups ...string) (int, string) {
			c, rec := aiContext(http.MethodDelete, "/nodes/n1", "", groups...)
			if err := a.DeleteNode(c, "n1"); err == nil {
				t.Errorf("DeleteNode: no error once denied")
			}
			return rec.Code, rec.Body.String()
		}},
		{"POST /nodes/delete", func(a *Api, groups ...string) (int, string) {
			c, rec := aiContext(http.MethodPost, "/nodes/delete", `{"ids":["n1"]}`, groups...)
			if err := a.PostNodesDelete(c); err == nil {
				t.Errorf("PostNodesDelete: no error once denied")
			}
			return rec.Code, rec.Body.String()
		}},
		{"DELETE /clusters/{cluster_id}", func(a *Api, groups ...string) (int, string) {
			c, rec := aiContext(http.MethodDelete, "/clusters/c1", "", groups...)
			if err := a.DeleteCluster(c, "c1"); err == nil {
				t.Errorf("DeleteCluster: no error once denied")
			}
			return rec.Code, rec.Body.String()
		}},
	} {
		code, body := tc.call(a, "ops")
		if code != http.StatusForbidden || body != `{"text":"NodeManager privilege required"}`+"\n" {
			t.Errorf("%s: %d %q, want a single 403", tc.what, code, body)
		}
	}
}
