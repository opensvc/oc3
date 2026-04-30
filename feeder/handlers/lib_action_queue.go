package feederhandlers

import (
	"context"

	"github.com/opensvc/oc3/cdb"
)

func (a *Api) getNodeWithActionQueued(ctx context.Context, clusterID string) (nodeL []string, err error) {
	var actions []cdb.ActionQueueNamedEntry
	actions, err = a.ODB.ActionQByClusterID(ctx, clusterID)
	if err != nil {
		return
	} else if len(actions) > 0 {
		nodeM := make(map[string]struct{})
		for _, a := range actions {
			if a.ConnectTo != nil && *a.ConnectTo != "" {
				nodeM[*a.ConnectTo] = struct{}{}
			}
		}
		for nodename := range nodeM {
			nodeL = append(nodeL, nodename)
		}
	}
	return
}
