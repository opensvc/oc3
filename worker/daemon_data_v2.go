package worker

import (
	"fmt"
	"strings"
)

type (
	daemonDataV2 struct {
		data data
	}

	data map[string]any

	instanceStatus struct {
		DBInstanceStatus

		encap     map[string]any
		resources map[string]any

		// fromOutsideStatus is the resource status of self from the parent hypervisor:
		//    nodes.<parent-hypervisor>.services.status.<svc>.resources.<containerID>.status
		fromOutsideStatus string
	}
)

func (d data) getString(key string) (s string, err error) {
	if i, ok := d[key]; !ok {
		err = fmt.Errorf("invalid schema: no key '%s'", key)
	} else if s, ok = i.(string); !ok {
		err = fmt.Errorf("invalid schema: %s not a string", key)
	} else if len(s) == 0 {
		err = fmt.Errorf("unexpected empty %s value", key)
	}
	return
}

func (d data) getDictSubKeys(key string) (l []string, err error) {
	m, ok := d[key].(map[string]any)
	if !ok {
		err = fmt.Errorf("invalid schema: no key '%s'", key)
		return
	}
	for s := range m {
		l = append(l, s)
	}
	return
}

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

func mapTo(m map[string]any, k ...string) (any, bool) {
	if len(k) <= 1 {
		v, ok := m[k[0]]
		return v, ok
	}
	if v, ok := m[k[0]].(map[string]any); !ok {
		return v, ok
	} else {
		return mapTo(v, k[1:]...)
	}
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

func mapToA(m map[string]any, defaultValue any, k ...string) any {
	if v, ok := mapTo(m, k...); ok && v != nil {
		return v
	} else {
		return defaultValue
	}
}

func mapToBool(m map[string]any, defaultValue bool, k ...string) bool {
	return mapToA(m, defaultValue, k...).(bool)
}

// mapToBoolS returns "T" or "F"
func mapToBoolS(m map[string]any, defaultValue bool, k ...string) string {
	if mapToBool(m, defaultValue, k...) {
		return "T"
	} else {
		return "F"
	}
}

func mapToS(m map[string]any, defaultValue string, k ...string) string {
	return mapToA(m, defaultValue, k...).(string)
}

func mapToMap(m map[string]any, defaultValue map[string]any, k ...string) map[string]any {
	return mapToA(m, defaultValue, k...).(map[string]any)
}

func (d *daemonDataV2) InstanceStatus(objectName string, nodename string) *instanceStatus {
	var a, nilMap map[string]any
	if i, ok := mapTo(d.data, "nodes", nodename, "services", "status", objectName); !ok {
		return nil
	} else if a, ok = i.(map[string]any); !ok {
		return nil
	}
	instanceStatus := &instanceStatus{DBInstanceStatus: DBInstanceStatus{}}

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
	return instanceStatus
}

func aToInstanceResource(a map[string]any, svcID, nodeID, vmName, rID string) *DBInstanceResource {
	res := &DBInstanceResource{
		svcID:    svcID,
		nodeID:   nodeID,
		vmName:   vmName,
		rid:      rID,
		status:   mapToS(a, "", "status"),
		desc:     mapToS(a, "", "label"),
		disable:  mapToBoolS(a, false, "disable"),
		monitor:  mapToBoolS(a, false, "monitor"),
		optional: mapToBoolS(a, false, "optional"),
		resType:  mapToS(a, "", "type"),
	}
	if logs, ok := mapTo(a, "log"); ok {
		switch l := logs.(type) {
		case []string:
			res.log = strings.Join(l, "\n")
		}
	}
	return res
}

func (i *instanceStatus) InstanceResources() []*DBInstanceResource {
	l := make([]*DBInstanceResource, len(i.resources))
	id := 0
	for rID, aResource := range i.resources {
		var vmName string
		if i.encap != nil {
			if encapRid, ok := i.encap[rID].(map[string]any); ok {
				if s, ok := encapRid["hostname"].(string); ok {
					vmName = s
				}
			}
		}
		l[id] = aToInstanceResource(aResource.(map[string]any), i.svcID, i.nodeID, vmName, rID)
		id++
	}
	return l
}

// Containers returns list of container instanceStatus that are defined by i.encap.
func (i *instanceStatus) Containers() []*instanceStatus {
	l := make([]*instanceStatus, len(i.encap))
	id := 0
	for containerID := range i.encap {
		l[id] = i.Container(containerID)
		id++
	}
	return l
}

// Container returns the container instance status of i from i.encap[id].
func (i *instanceStatus) Container(id string) *instanceStatus {
	encap := i.encap[id].(map[string]any)
	if encap == nil {
		return nil
	}
	dbI := DBInstanceStatus{
		nodeID: i.nodeID,
		svcID:  i.svcID,
	}
	if vmName, ok := encap["hostname"].(string); ok {
		dbI.monVmType = vmName
	}
	if containerType := strings.SplitN(mapToS(i.resources, "", id, "type"), ".", 1); len(containerType) > 1 {
		dbI.monVmType = containerType[1]
	}
	mergeM := hypervisorContainerMergeMap
	if encapAvail, ok := encap["avail"].(string); ok {
		dbI.monAvailStatus = mergeM[i.monAvailStatus+","+encapAvail]
	} else {
		dbI.monAvailStatus = mergeM[i.monAvailStatus+",n/a"]
	}
	if encapOverall, ok := encap["overall"].(string); ok {
		dbI.monOverallStatus = mergeM[i.monOverallStatus+","+encapOverall]
	} else {
		dbI.monOverallStatus = mergeM[i.monOverallStatus+",n/a"]
	}

	if statusGroup, ok := encap["status_group"].(map[string]string); ok {
		dbI.monIpStatus = mergeM[i.monIpStatus+","+statusGroup["ip"]]
		dbI.monDiskStatus = mergeM[i.monDiskStatus+","+statusGroup["disk"]]
		dbI.monFsStatus = mergeM[i.monFsStatus+","+statusGroup["fs"]]
		dbI.monShareStatus = mergeM[i.monShareStatus+","+statusGroup["share"]]
		dbI.monContainerStatus = mergeM[i.monContainerStatus+","+statusGroup["container"]]
		dbI.monAppStatus = mergeM[i.monAppStatus+","+statusGroup["app"]]
		dbI.monSyncStatus = mergeM[i.monSyncStatus+","+statusGroup["sync"]]
	} else {
		// unexpected status_group map, ignore all encap status_group
		dbI.monIpStatus = mergeM[i.monIpStatus+","]
		dbI.monDiskStatus = mergeM[i.monDiskStatus+","]
		dbI.monFsStatus = mergeM[i.monFsStatus+","]
		dbI.monShareStatus = mergeM[i.monShareStatus+","]
		dbI.monContainerStatus = mergeM[i.monContainerStatus+","]
		dbI.monAppStatus = mergeM[i.monAppStatus+","]
		dbI.monSyncStatus = mergeM[i.monSyncStatus+","]
	}

	// frozen value merge rules:
	//    0: global thawed + encap thawed
	//    1: global frozen + encap thawed
	//    2: global thawed + encap frozen
	//    3: global frozen + encap frozen

	encapFrozen, _ := encap["frozen"].(int)
	switch encapFrozen {
	case 0:
		// encap is thawed => frozen result is the hypervisor frozen value
		dbI.monFrozen = i.monFrozen
	default:
		// encap is frozen => frozen result is the global frozen value + 2
		dbI.monFrozen = i.monFrozen + 2
	}

	var nilMap map[string]any
	return &instanceStatus{
		DBInstanceStatus:  dbI,
		resources:         mapToMap(encap, nilMap, "resources"),
		fromOutsideStatus: mapToS(i.resources, "", id, "status"),
	}
}
