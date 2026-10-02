package feederhandlers

import (
	"context"
	"errors"
	"testing"
)

func TestReportedNodeID(t *testing.T) {
	nodes := map[string]string{"cluster1/dev1n2": "id2"}
	var lookups int
	lookup := func(_ context.Context, clusterID, nodename string) (string, error) {
		lookups++
		if nodename == "broken" {
			return "", errors.New("db down")
		}
		return nodes[clusterID+"/"+nodename], nil
	}
	ptr := func(s string) *string { return &s }

	for name, tc := range map[string]struct {
		clusterID string
		nodename  *string
		want      string
		wantErr   error
		anyErr    bool
		noLookup  bool
	}{
		"own data, no nodename":                 {clusterID: "cluster1", want: "id1", noLookup: true},
		"own data, empty nodename":              {clusterID: "cluster1", nodename: ptr(""), want: "id1", noLookup: true},
		"own data, named":                       {clusterID: "cluster1", nodename: ptr("DEV1N1"), want: "id1", noLookup: true},
		"a node of the cluster":                 {clusterID: "cluster1", nodename: ptr("dev1n2"), want: "id2"},
		"a node out of the cluster":             {clusterID: "cluster1", nodename: ptr("other"), wantErr: errNodeNotInCluster},
		"a node of another cluster":             {clusterID: "cluster2", nodename: ptr("dev1n2"), wantErr: errNodeNotInCluster},
		"an authenticated node without cluster": {nodename: ptr("dev1n2"), wantErr: errNodeNotInCluster, noLookup: true},
		"a lookup failing":                      {clusterID: "cluster1", nodename: ptr("broken"), anyErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			lookups = 0
			got, err := reportedNodeID(context.Background(), lookup, "id1", "dev1n1", tc.clusterID, tc.nodename)
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("got error %v, want %v", err, tc.wantErr)
				}
				if errors.Is(err, errNodeNotInCluster) && tc.nodename != nil && got != "" {
					t.Errorf("got node id %q with an error", got)
				}
			case tc.anyErr:
				if err == nil || errors.Is(err, errNodeNotInCluster) {
					t.Fatalf("got error %v, want a lookup error", err)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				if got != tc.want {
					t.Errorf("got %q, want %q", got, tc.want)
				}
			}
			if tc.noLookup && lookups > 0 {
				t.Errorf("unexpected lookup")
			}
		})
	}
}
