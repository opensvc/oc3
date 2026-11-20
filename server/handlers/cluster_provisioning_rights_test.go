package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
)

type fakeProvisioningRights struct {
	userApps map[string][]string
	nodeApps map[string][]string
}

func (f fakeProvisioningRights) AppsForGroups(_ context.Context, groups []string) ([]string, error) {
	var l []string
	for _, g := range groups {
		l = append(l, f.userApps[g]...)
	}
	return l, nil
}

func (f fakeProvisioningRights) ClusterNodeApps(_ context.Context, clusterID string) ([]string, error) {
	return f.nodeApps[clusterID], nil
}

// TestRequireProvisioningApps checks a NodeManager provisions a cluster only
// responsible for the apps declared and for every node the collector has in
// it: the members of a cluster enroll the nodes bootstrapped into it.
func TestRequireProvisioningApps(t *testing.T) {
	store := fakeProvisioningRights{
		userApps: map[string][]string{"TeamA": {"A"}, "TeamB": {"B"}},
		nodeApps: map[string][]string{"ca": {"A"}, "cab": {"A", "B"}},
	}
	for _, tc := range []struct {
		name    string
		groups  []string
		cluster string
		apps    []string
		want    int
	}{
		{"new cluster, own app", []string{"NodeManager", "TeamA"}, "", []string{"A"}, 0},
		{"new cluster, other app", []string{"NodeManager", "TeamA"}, "", []string{"B"}, http.StatusForbidden},
		{"own cluster", []string{"NodeManager", "TeamA"}, "ca", []string{"A", ""}, 0},
		{"cluster of another app", []string{"NodeManager", "TeamB"}, "ca", []string{"B"}, http.StatusForbidden},
		{"cluster partly of another app", []string{"NodeManager", "TeamA"}, "cab", nil, http.StatusForbidden},
		{"both apps", []string{"NodeManager", "TeamA", "TeamB"}, "cab", []string{"A"}, 0},
		{"Manager", []string{"Manager"}, "cab", []string{"Z"}, 0},
		{"no privilege", []string{"TeamA"}, "ca", []string{"A"}, http.StatusForbidden},
	} {
		c, rec := aiContext(http.MethodPost, "/clusters", "", tc.groups...)
		err := requireProvisioningApps(c, context.Background(), slog.Default(), store, tc.cluster, tc.apps...)
		got := 0
		if err != nil {
			got = rec.Code
		}
		if got != tc.want {
			t.Errorf("%s: got %d, want %d (%s)", tc.name, got, tc.want, rec.Body.String())
		}
	}
}

// TestRequireProvisioningVisible checks the declaration of a cluster the user
// may not provision is not found, as if it did not exist.
func TestRequireProvisioningVisible(t *testing.T) {
	store := fakeProvisioningRights{
		userApps: map[string][]string{"TeamA": {"A"}, "TeamB": {"B"}},
		nodeApps: map[string][]string{"ca": {"A"}},
	}
	for _, tc := range []struct {
		groups []string
		app    string
		want   int
	}{
		{[]string{"NodeManager", "TeamA"}, "A", 0},
		{[]string{"NodeManager", "TeamB"}, "A", http.StatusNotFound},
		{[]string{"NodeManager", "TeamB"}, "B", http.StatusNotFound}, // nodes of A
		{[]string{"Manager"}, "B", 0},
	} {
		c, rec := aiContext(http.MethodGet, "/clusters/ca/declaration", "", tc.groups...)
		got := 0
		if err := requireProvisioningVisible(c, context.Background(), slog.Default(), store, "ca", tc.app); err != nil {
			got = rec.Code
		}
		if got != tc.want {
			t.Errorf("%v app %s: got %d, want %d", tc.groups, tc.app, got, tc.want)
		}
	}
}
