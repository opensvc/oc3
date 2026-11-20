package serverhandlers

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// TestNodeSelfUpdateRefused checks a node can not change on itself what the
// rights rest on, before any lookup: the Api has no database.
func TestNodeSelfUpdateRefused(t *testing.T) {
	for _, key := range nodeSelfReadOnlyFields {
		c, rec := aiContext(http.MethodPost, "/nodes/n1", "")
		c.Set(XAuthMode, AuthModeNode)
		c.Set(XNodeID, "n1")
		_ = postNodeUpdate(c, nil, nil, context.Background(), "n1", map[string]any{key: "x", "loc_city": "Paris"})
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), key) {
			t.Errorf("%s: %d %s", key, rec.Code, rec.Body.String())
		}
	}
}

// TestNodeClusterIDChangeManagerOnly checks only a Manager sets the cluster
// of a node: a NodeManager would make it pass for a member of any cluster.
func TestNodeClusterIDChangeManagerOnly(t *testing.T) {
	for _, tc := range []struct {
		groups []string
		fields map[string]any
		denied bool
	}{
		{[]string{"NodeManager"}, map[string]any{"cluster_id": "c1"}, true},
		{[]string{"NodeManager"}, map[string]any{"loc_city": "Paris"}, false},
		{[]string{"Manager"}, map[string]any{"cluster_id": "c1"}, false},
	} {
		c, rec := aiContext(http.MethodPost, "/nodes/n1", "", tc.groups...)
		err := checkNodeClusterIDChange(c, tc.fields)
		if denied := err != nil; denied != tc.denied || (denied && rec.Code != http.StatusForbidden) {
			t.Errorf("%v %v: denied %v (%d), want %v", tc.groups, tc.fields, denied, rec.Code, tc.denied)
		}
	}
}
