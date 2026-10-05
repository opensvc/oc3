package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeSan handles GET /nodes/{node_id}/san
func (a *Api) GetNodeSan(c echo.Context, nodeId string) error {
	log := echolog.GetLogHandler(c, "GetNodeSan")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	endpoints, err := a.ODB.NodeSANEndpoints(ctx, node.NodeID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot read the san endpoints", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the san endpoints of node %s", nodeId)
	}
	var topology server.SanTopology
	if len(endpoints) == 0 {
		// No adapter, or none the caller may see: nothing to follow in the switches.
		topology = sanTopology(node.NodeID, node.Nodename, nil, nil)
	} else {
		ports, err := a.ODB.SwitchPorts(ctx)
		if err != nil {
			log.Error("cannot read the switch ports", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot read the switch ports")
		}
		topology = sanTopology(node.NodeID, node.Nodename, endpoints, ports)
	}
	return c.JSON(http.StatusOK, server.SanTopologyResponse{Data: topology})
}
