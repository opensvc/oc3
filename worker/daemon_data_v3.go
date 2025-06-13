package worker

import (
	"fmt"
	"strings"
	"time"
)

type (
	daemonDataV3 struct {
		data    data
		cluster data
	}
)

func (d *daemonDataV3) objectNames() (l []string, err error) {
	var keys []string
	keys, err = d.cluster.getDictSubKeys("object")
	if err != nil {
		return
	}
	for _, s := range keys {
		if s == "cluster" {
			continue
		}
		l = append(l, s)
	}
	return
}

func (d *daemonDataV3) nodeNames() ([]string, error) {
	return d.cluster.getDictSubKeys("node")
}

func (d *daemonDataV3) clusterID() (s string, err error) {
	if i, ok := mapTo(d.cluster, "config", "id"); ok {
		if s, ok = i.(string); ok {
			return
		}
		err = fmt.Errorf("data v3 got unexpected cluster id type")
		return
	}
	err = fmt.Errorf("data v3 no such key: cluster.id")
	return
}

func (d *daemonDataV3) clusterName() (s string, err error) {
	if i, ok := mapTo(d.cluster, "config", "name"); ok {
		if s, ok = i.(string); ok {
			return
		}
		err = fmt.Errorf("data v3 got unexpected cluster name type")
		return
	}
	err = fmt.Errorf("data v3 no such key: cluster.name")
	return
}

func (d *daemonDataV3) nodeFrozen(nodename string) (string, error) {
	i, ok := mapTo(d.cluster, "node", nodename, "status", "frozen_at")
	if !ok {
		return "", fmt.Errorf("data v3 no such key: node.%s.status.frozen_at", nodename)
	} else {
		if v, ok := i.(string); ok {
			if frozenAt, err := time.Parse(time.RFC3339Nano, v); err != nil {
				return "", fmt.Errorf("data v3 got unexpected node frozen at time: %s frozen_at value %s",
					nodename, v)
			} else if frozenAt.After(time.Time{}) {
				return "T", nil
			} else {
				return "F", nil
			}
		} else {
			return "", fmt.Errorf("data v3 got unexpected node frozen at value: %s", nodename)
		}
	}
}

func (d *daemonDataV3) nodeHeartbeat(nodename string) ([]heartbeatData, error) {
	i, ok := mapTo(d.cluster, "node", nodename, "daemon", "heartbeat", "streams")
	if !ok {
		return nil, fmt.Errorf("data v3 no such key: node.%s.daemon.heartbeat.streams", nodename)
	}
	iL, ok := i.([]any)
	if !ok {
		return nil, fmt.Errorf("data v3 unexpected value for key node.%s.daemon.heartbeat.streams", nodename)
	}
	l := make([]heartbeatData, 0, len(iL))
	var nilMap map[string]any
	for _, v := range iL {
		stream, ok := v.(map[string]any)
		if !ok {
			continue
		}
		name := mapToS(stream, "", "id")
		if name == "" {
			return nil, fmt.Errorf("data v3 unexpected empty stream id for key node.%s.daemon.heartbeat.streams", nodename)
		}
		name = strings.TrimPrefix(name, "hb#")
		family := mapToS(stream, "", "type")
		state := mapToS(stream, "", "state")

		// Add entry for the node hb state itself regardless of its peers
		l = append(l, heartbeatData{
			DBHeartbeat: DBHeartbeat{
				nodeID: "",
				driver: family,
				name:   name,
				state:  state,
			},
			nodename: nodename,
		})

		if state != "running" {
			continue
		}
		for peer, i := range mapToMap(stream, nilMap, "peers") {
			v, ok := i.(map[string]any)
			if !ok {
				continue
			}
			var beating int8
			if mapToBool(v, false, "is_beating") {
				beating = 1
			} else {
				beating = 2
			}
			lastBeating, _ := time.Parse(time.RFC3339Nano, mapToS(v, "", "last_at"))
			l = append(l, heartbeatData{
				DBHeartbeat: DBHeartbeat{
					driver:      family,
					name:        name,
					state:       state,
					beating:     beating,
					desc:        mapToS(v, "", "desc"),
					lastBeating: lastBeating,
				},
				nodename:     nodename,
				peerNodename: peer,
			})
		}
	}
	return l, nil
}

func (d *daemonDataV3) appFromObjectName(objectName string, nodes ...string) string {
	for _, nodename := range nodes {
		if a, ok := mapTo(d.cluster, "node", nodename, "instance", objectName, "config", "app"); ok {
			if app, ok := a.(string); ok {
				return app
			}
		}
	}
	return ""
}

func (d *daemonDataV3) objectStatus(objectName string) *DBObjStatus {
	if i, ok := mapTo(d.cluster, "object", objectName); ok && i != nil {
		if o, ok := i.(map[string]any); ok {
			oStatus := &DBObjStatus{
				availStatus:   "n/a",
				overallStatus: "n/a",
				placement:     "n/a",
				frozen:        "n/a",
				provisioned:   "n/a",
			}
			if s, ok := o["avail"].(string); ok {
				oStatus.availStatus = s
			}
			if s, ok := o["overall"].(string); ok {
				oStatus.overallStatus = s
			}
			if s, ok := o["placement_state"].(string); ok {
				oStatus.placement = s
			}
			if s, ok := o["frozen"].(string); ok {
				oStatus.frozen = s
			}
			if prov, ok := o["provisioned"].(string); ok {
				if prov == "true" {
					oStatus.provisioned = "True"
				} else {
					oStatus.provisioned = "False"
				}
			}
			return oStatus
		}
	}
	return nil
}

func (d *daemonDataV3) InstanceStatus(objectName string, nodename string) *instanceData {
	var config, monitor, status, nilMap map[string]any
	if i, ok := mapTo(d.cluster, "node", nodename, "instance", objectName, "status"); !ok {
		return nil
	} else if status, ok = i.(map[string]any); !ok {
		return nil
	}

	if i, ok := mapTo(d.cluster, "node", nodename, "instance", objectName, "monitor"); !ok {
		return nil
	} else if monitor, ok = i.(map[string]any); !ok {
		return nil
	}

	if i, ok := mapTo(d.cluster, "node", nodename, "instance", objectName, "config"); !ok {
		return nil
	} else if config, ok = i.(map[string]any); !ok {
		return nil
	}

	instanceStatus := &instanceData{
		DBInstanceStatus:  DBInstanceStatus{},
		resourceMonitored: make(map[string]bool),
	}

	instanceStatus.monAvailStatus = mapToS(status, "", "avail")

	instanceStatus.monOverallStatus = mapToS(status, "", "overall")

	// TODO: verify v3 encap
	instanceStatus.encap = mapToMap(status, nilMap, "encap")

	instanceStatus.resources = mapToMap(status, nilMap, "resources")

	if frozenAt, _ := time.Parse(time.RFC3339Nano, status["frozen_at"].(string)); frozenAt.After(time.Time{}) {
		instanceStatus.monFrozen = 1
	}

	// TODO: verify defaults
	instanceStatus.monSmonStatus = mapToS(monitor, "", "state")
	instanceStatus.monSmonGlobalExpect = mapToS(monitor, "", "global_expect")

	// TODO: status group from v2 (ip/disk/fs/share/container/app/sync)?
	instanceStatus.monIpStatus = mapToS(status, "n/a", "status_group", "ip")
	instanceStatus.monDiskStatus = mapToS(status, "n/a", "status_group", "disk")
	instanceStatus.monFsStatus = mapToS(status, "n/a", "status_group", "fs")
	instanceStatus.monShareStatus = mapToS(status, "n/a", "status_group", "share")
	instanceStatus.monContainerStatus = mapToS(status, "n/a", "status_group", "container")
	instanceStatus.monAppStatus = mapToS(status, "n/a", "status_group", "app")
	instanceStatus.monSyncStatus = mapToS(status, "n/a", "status_group", "sync")

	configResources := mapToMap(config, nilMap, "resources")
	for rid := range instanceStatus.resources {
		instanceStatus.resourceMonitored[rid] = mapToBool(configResources, false, rid, "is_monitored")
	}

	return instanceStatus
}
