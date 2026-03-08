package worker

import (
	"context"

	"github.com/opensvc/oc3/cdb"
)

type (
	instanceDetails struct {
		resmonL []*cdb.DBInstanceResource
		svcmonL []*cdb.DBInstanceStatus

		// cVmNameL is the list of container vm names that needs update from the associated node
		cVmNameL []*cdb.ContainerNode

		MonSmonStatus string

		pingInstance bool
		dropInstance bool
	}
)

// extractInstanceDetails processes and extracts detailed information about a specific instance based on its status, object, node, and changes.
func (d *JobDB) extractInstanceDetails(ctx context.Context, iStatus *instanceData, obj *cdb.DBObject, node *cdb.DBNode, changes map[string]struct{}) (result instanceDetails, err error) {
	objID := obj.SvcID
	objectName := obj.Svcname
	nodeID := node.NodeID
	nodename := node.Nodename
	iStatus.SvcID = objID
	iStatus.NodeID = nodeID
	_, isChanged := changes[objectName+"@"+nodename]
	if !isChanged && obj.AvailStatus != "undef" && obj.AvailStatus == "TODO" {
		result.pingInstance = true
		return result, nil
	}
	result.MonSmonStatus = iStatus.MonSmonStatus
	if iStatus.encap == nil {
		subNodeID, _, _, err := d.oDb.TranslateEncapNodename(ctx, objID, nodeID)
		if err != nil {
			return result, err
		}
		if subNodeID != "" && subNodeID != nodeID {
			return result, nil
		}
		if iStatus.resources == nil {
			// scaler or wrapper, for example
			result.dropInstance = true
		} else {
			result.svcmonL = append(result.svcmonL, &iStatus.DBInstanceStatus)
			result.resmonL = append(result.resmonL, iStatus.InstanceResources()...)
		}
	} else {
		if iStatus.resources == nil {
			// scaler or wrapper, for example
			result.dropInstance = true
		} else {
			for _, containerStatus := range iStatus.Containers() {
				if containerStatus == nil {
					continue
				}
				if containerStatus.fromOutsideStatus == "up" {
					result.cVmNameL = append(result.cVmNameL, &cdb.ContainerNode{DBNode: node, MonVmName: containerStatus.MonVmName, ObjApp: obj.App, ObjID: objID, ObjName: objectName})
				}

				result.svcmonL = append(result.svcmonL, &containerStatus.DBInstanceStatus)
				result.resmonL = append(result.resmonL, iStatus.InstanceResources()...)
			}
		}
	}

	// TODO: verify if we need a placement non optimal alert for object/instance
	//     om2 has: monitor.services.'<path>'.placement = non-optimal
	//     om3 has: cluster.object.<path>.placement_state = non-optimal
	//				cluster.node.<node>.instance.<path>.monitor.is_ha_leader
	//				cluster.node.<node>.instance.<path>.monitor.is_leader
	//     collector v2 calls update_dash_service_not_on_primary (broken since no DEFAULT.autostart_node values)
	return result, nil
}
