package worker

import (
	"fmt"
	"strings"
	"time"
)

type (
	daemonDataV2 struct {
		data data
	}
)

func (d *daemonDataV2) nodeNames() (l []string, err error) {
	return d.data.getDictSubKeys("nodes")
}

func (d *daemonDataV2) objectNames() (l []string, err error) {
	var keys []string
	keys, err = d.data.getDictSubKeys("services")
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

func (d *daemonDataV2) clusterID() (s string, err error) {
	return d.data.getString("cluster_id")
}

func (d *daemonDataV2) clusterName() (s string, err error) {
	return d.data.getString("cluster_name")
}

func (d *daemonDataV2) parseNodeFrozen(i any) string {
	switch v := i.(type) {
	case int:
		if v > 0 {
			return "T"
		}
	case float64:
		if v > 0 {
			return "T"
		}
	}
	return "F"
}

func (d *daemonDataV2) nodeFrozen(nodename string) (s string, err error) {
	i, ok := mapTo(d.data, "nodes", nodename, "frozen")
	if !ok {
		err = fmt.Errorf("can't retrieve frozen for %s", nodename)
		return
	} else {
		return d.parseNodeFrozen(i), nil
	}
}

func (d *daemonDataV2) nodeHeartbeat(nodename string) ([]heartbeatData, error) {
	i, ok := mapTo(d.data, "nodes", nodename, "hb")
	if !ok {
		return nil, fmt.Errorf("data v2 no such key: nodes.%s.hb", nodename)
	}
	iM, ok := i.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("data v2 unexpected value for key nodes.%s.hb", nodename)
	}
	var l []heartbeatData
	var nilMap map[string]any
	for name, v := range iM {
		stream, ok := v.(map[string]any)
		if !ok {
			continue
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
			if _, ok := mapTo(v, "beating"); !ok {
				continue
			}
			var beating int8
			if mapToBool(v, false, "beating") {
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

func (d *daemonDataV2) getFromKeys(keys ...string) (v any, err error) {
	if v, ok := mapTo(d.data, keys...); !ok {
		return v, fmt.Errorf("getFromKeys can't expand from %v", keys)
	} else {
		return v, nil
	}
}

// appFromObjectName returns object app value from nodes object instances status
func (d *daemonDataV2) appFromObjectName(svcname string, nodes ...string) string {
	for _, nodename := range nodes {
		if a, ok := mapTo(d.data, "nodes", nodename, "services", "status", svcname, "app"); ok {
			if app, ok := a.(string); ok {
				return app
			}
		}
	}
	return ""
}

func (d *daemonDataV2) objectStatus(objectName string) *DBObjStatus {
	if i, ok := mapTo(d.data, "services", objectName); ok && i != nil {
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
			if s, ok := o["placement"].(string); ok {
				oStatus.placement = s
			}
			if s, ok := o["frozen"].(string); ok {
				oStatus.frozen = s
			}
			if prov, ok := o["provisioned"].(bool); ok {
				if prov {
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

func (d *daemonDataV2) InstanceStatus(objectName string, nodename string) *instanceData {
	var a, nilMap map[string]any
	if i, ok := mapTo(d.data, "nodes", nodename, "services", "status", objectName); !ok {
		return nil
	} else if a, ok = i.(map[string]any); !ok {
		return nil
	}
	instanceStatus := &instanceData{
		DBInstanceStatus:  DBInstanceStatus{},
		resourceMonitored: make(map[string]bool),
	}

	instanceStatus.monSmonStatus = mapToS(a, "", "monitor", "status")
	instanceStatus.monSmonGlobalExpect = mapToS(a, "", "monitor", "global_expect")
	instanceStatus.monAvailStatus = mapToS(a, "", "avail")
	instanceStatus.monOverallStatus = mapToS(a, "", "overall")
	instanceStatus.monIpStatus = mapToS(a, "n/a", "status_group", "ip")
	instanceStatus.monDiskStatus = mapToS(a, "n/a", "status_group", "disk")
	instanceStatus.monFsStatus = mapToS(a, "n/a", "status_group", "fs")
	instanceStatus.monShareStatus = mapToS(a, "n/a", "status_group", "share")
	instanceStatus.monContainerStatus = mapToS(a, "n/a", "status_group", "container")
	instanceStatus.monAppStatus = mapToS(a, "n/a", "status_group", "app")
	instanceStatus.monSyncStatus = mapToS(a, "n/a", "status_group", "sync")
	instanceStatus.encap = mapToMap(a, nilMap, "encap")
	instanceStatus.resources = mapToMap(a, nilMap, "resources")

	switch v := mapToA(a, 0, "frozen").(type) {
	case int:
		if v > 0 {
			instanceStatus.monFrozen = 1
		}
	case float64:
		if v > 0 {
			instanceStatus.monFrozen = 1
		}
	default:
		instanceStatus.monFrozen = 1
	}

	for rid := range instanceStatus.resources {
		instanceStatus.resourceMonitored[rid] = mapToBool(instanceStatus.resources, false, rid, "monitor")
	}
	return instanceStatus
}
