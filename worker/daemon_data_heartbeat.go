package worker

import "fmt"

type (
	heartbeatData struct {
		DBHeartbeat

		nodename     string
		peerNodename string
	}
)

func (d heartbeatData) String() string {
	return fmt.Sprintf("%s:%s - %s:%s %s state:%s beating:%d",
		d.nodename, d.DBHeartbeat.nodeID, d.peerNodename, d.DBHeartbeat.peerNodeID,
		d.DBHeartbeat.name, d.DBHeartbeat.state, d.DBHeartbeat.beating)
}
