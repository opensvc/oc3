package worker

import (
	"strings"
)

type (
	// instanceData
	instanceData struct {
		DBInstanceStatus

		encap     map[string]any
		resources map[string]any

		// resourceMonitored map of rid to config resourceMonitored value
		resourceMonitored map[string]bool

		// fromOutsideStatus is the resource status of self from the parent hypervisor:
		//    nodes.<parent-hypervisor>.services.status.<svc>.resources.<containerID>.status
		fromOutsideStatus string
	}
)

func (i *instanceData) InstanceResources() []*DBInstanceResource {
	var l []*DBInstanceResource
	for rID, aResource := range i.resources {
		l = append(l, aToInstanceResource(aResource.(map[string]any), i.svcID, i.nodeID, "", rID,
			i.resourceMonitored[rID]))

		if i.encap != nil {
			if encapRid, ok := i.encap[rID].(map[string]any); ok {
				var vmName string
				if s, ok := encapRid["hostname"].(string); ok {
					vmName = s
				}
				if encapResources, ok := encapRid["resources"].(map[string]any); ok {
					for encapRID, encapAResource := range encapResources {
						l = append(l, aToInstanceResource(encapAResource.(map[string]any), i.svcID, i.nodeID, vmName, encapRID,
							i.resourceMonitored[encapRID]))
					}
				}
			}
		}
	}
	return l
}

func aToInstanceResource(a map[string]any, svcID, nodeID, vmName, rID string, monitor bool) *DBInstanceResource {
	res := &DBInstanceResource{
		svcID:    svcID,
		nodeID:   nodeID,
		vmName:   vmName,
		rid:      rID,
		status:   mapToS(a, "", "status"),
		desc:     mapToS(a, "", "label"),
		disable:  mapToBoolS(a, false, "disable"),
		optional: mapToBoolS(a, false, "optional"),
		resType:  mapToS(a, "", "type"),
	}
	if monitor {
		res.monitor = "T"
	} else {
		res.monitor = "F"
	}
	if logs, ok := mapTo(a, "log"); ok {
		switch l := logs.(type) {
		case []string:
			// v2 log entry
			res.log = strings.Join(l, "\n")
		case []map[string]string:
			// v3 log entry
			lines := make([]string, 0, len(l))
			for _, m := range l {
				lines = append(lines, m["level"]+": "+m["message"])
			}
			res.log = strings.Join(lines, "\n")
		}
	}
	return res
}

// Containers returns list of container instanceData that are defined by i.encap.
func (i *instanceData) Containers() []*instanceData {
	l := make([]*instanceData, len(i.encap))
	id := 0
	for containerID := range i.encap {
		l[id] = i.Container(containerID)
		id++
	}
	return l
}

// Container returns the container instance status of i from i.encap[id].
func (i *instanceData) Container(id string) *instanceData {
	encap := i.encap[id].(map[string]any)
	if encap == nil {
		return nil
	}
	dbI := DBInstanceStatus{
		nodeID: i.nodeID,
		svcID:  i.svcID,
	}
	if vmName, ok := encap["hostname"].(string); ok {
		dbI.monVmName = vmName
	}
	// defines vm type from resources.<container#xx>.type = 'container.podman' -> podman
	if containerType := strings.SplitN(mapToS(i.resources, "", id, "type"), ".", -1); len(containerType) > 1 {
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
	return &instanceData{
		DBInstanceStatus:  dbI,
		resources:         mapToMap(encap, nilMap, "resources"),
		fromOutsideStatus: mapToS(i.resources, "", id, "status"),
	}
}
