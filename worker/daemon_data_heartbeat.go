package worker

import (
	"fmt"

	"github.com/opensvc/oc3/cdb"
)

type (
	heartbeatData struct {
		cdb.DBHeartbeat

		nodename     string
		peerNodename string
	}
)

func (d heartbeatData) String() string {
	return fmt.Sprintf("%s:%s - %s:%s %s state:%s beating:%d",
		d.nodename, d.DBHeartbeat.NodeID, d.peerNodename, d.DBHeartbeat.PeerNodeID,
		d.DBHeartbeat.Name, d.DBHeartbeat.State, d.DBHeartbeat.Beating)
}
