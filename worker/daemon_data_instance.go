package worker

import (
	"strings"

	"github.com/opensvc/oc3/cdb"
)

type (
	// instanceData
	instanceData struct {
		cdb.DBInstanceStatus

		encap     map[string]any
		resources map[string]any

		// resourceMonitored map of rid to config resourceMonitored value
		resourceMonitored map[string]bool

		// fromOutsideStatus is the resource status of self from the parent hypervisor:
		//    nodes.<parent-hypervisor>.services.status.<svc>.resources.<containerID>.status
		fromOutsideStatus string
	}
)

func (i *instanceData) InstanceResources() []*cdb.DBInstanceResource {
	var l []*cdb.DBInstanceResource
	for rID, aResource := range i.resources {
		l = append(l, aToInstanceResource(aResource.(map[string]any), i.SvcID, i.NodeID, "", rID,
			i.resourceMonitored[rID]))

		if i.encap != nil {
			if encapRid, ok := i.encap[rID].(map[string]any); ok {
				var vmName string
				if s, ok := encapRid["hostname"].(string); ok {
					vmName = s
				}
				if encapResources, ok := encapRid["resources"].(map[string]any); ok {
					for encapRID, encapAResource := range encapResources {
						l = append(l, aToInstanceResource(encapAResource.(map[string]any), i.SvcID, i.NodeID, vmName, encapRID,
							i.resourceMonitored[encapRID]))
					}
				}
			}
		}
	}
	return l
}

func aToInstanceResource(a map[string]any, svcID, nodeID, vmName, rID string, monitor bool) *cdb.DBInstanceResource {
	res := &cdb.DBInstanceResource{
		SvcID:    svcID,
		NodeID:   nodeID,
		VmName:   vmName,
		RID:      rID,
		Status:   mapToS(a, "", "status"),
		Desc:     mapToS(a, "", "label"),
		Disable:  mapToBoolS(a, false, "disable"),
		Optional: mapToBoolS(a, false, "optional"),
		ResType:  mapToS(a, "", "type"),
	}
	if monitor {
		res.Monitor = "T"
	} else {
		res.Monitor = "F"
	}
	if logs, ok := mapTo(a, "log"); ok {
		switch l := logs.(type) {
		case []string:
			// v2 log entry
			res.Log = strings.Join(l, "\n")
		case []map[string]string:
			// v3 log entry
			lines := make([]string, 0, len(l))
			for _, m := range l {
				lines = append(lines, m["level"]+": "+m["message"])
			}
			res.Log = strings.Join(lines, "\n")
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
	dbI := cdb.DBInstanceStatus{
		NodeID: i.NodeID,
		SvcID:  i.SvcID,
	}
	if vmName, ok := encap["hostname"].(string); ok {
		dbI.MonVmName = vmName
	}
	// defines vm type from resources.<container#xx>.type = 'container.podman' -> podman
	if containerType := strings.SplitN(mapToS(i.resources, "", id, "type"), ".", -1); len(containerType) > 1 {
		dbI.MonVmType = containerType[1]
	}
	mergeM := hypervisorContainerMergeMap
	if encapAvail, ok := encap["avail"].(string); ok {
		dbI.MonAvailStatus = mergeM[i.MonAvailStatus+","+encapAvail]
	} else {
		dbI.MonAvailStatus = mergeM[i.MonAvailStatus+",n/a"]
	}
	if encapOverall, ok := encap["overall"].(string); ok {
		dbI.MonOverallStatus = mergeM[i.MonOverallStatus+","+encapOverall]
	} else {
		dbI.MonOverallStatus = mergeM[i.MonOverallStatus+",n/a"]
	}

	if statusGroup, ok := encap["status_group"].(map[string]string); ok {
		dbI.MonIpStatus = mergeM[i.MonIpStatus+","+statusGroup["ip"]]
		dbI.MonDiskStatus = mergeM[i.MonDiskStatus+","+statusGroup["disk"]]
		dbI.MonFsStatus = mergeM[i.MonFsStatus+","+statusGroup["fs"]]
		dbI.MonShareStatus = mergeM[i.MonShareStatus+","+statusGroup["share"]]
		dbI.MonContainerStatus = mergeM[i.MonContainerStatus+","+statusGroup["container"]]
		dbI.MonAppStatus = mergeM[i.MonAppStatus+","+statusGroup["app"]]
		dbI.MonSyncStatus = mergeM[i.MonSyncStatus+","+statusGroup["sync"]]
	} else {
		// unexpected status_group map, ignore all encap status_group
		dbI.MonIpStatus = mergeM[i.MonIpStatus+","]
		dbI.MonDiskStatus = mergeM[i.MonDiskStatus+","]
		dbI.MonFsStatus = mergeM[i.MonFsStatus+","]
		dbI.MonShareStatus = mergeM[i.MonShareStatus+","]
		dbI.MonContainerStatus = mergeM[i.MonContainerStatus+","]
		dbI.MonAppStatus = mergeM[i.MonAppStatus+","]
		dbI.MonSyncStatus = mergeM[i.MonSyncStatus+","]
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
		dbI.MonFrozen = i.MonFrozen
	default:
		// encap is frozen => frozen result is the global frozen value + 2
		dbI.MonFrozen = i.MonFrozen + 2
	}

	var nilMap map[string]any
	return &instanceData{
		DBInstanceStatus:  dbI,
		resources:         mapToMap(encap, nilMap, "resources"),
		fromOutsideStatus: mapToS(i.resources, "", id, "status"),
	}
}
