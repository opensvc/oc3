package feederhandlers

import (
	"context"

	"github.com/opensvc/oc3/cdb"
)

func (a *Api) getNodeWithActionQueued(ctx context.Context, clusterID string) (nodeL []string, err error) {
	var actions []cdb.ActionQueueNamed
	actions, err = a.ODB.ActionQueueNamedByClusterID(ctx, clusterID)
	if err != nil {
		return
	} else if len(actions) > 0 {
		nodeM := make(map[string]struct{})
		for _, a := range actions {
			nodeM[a.Nodename] = struct{}{}
		}
		for nodename := range nodeM {
			nodeL = append(nodeL, nodename)
		}
	}
	return
}
