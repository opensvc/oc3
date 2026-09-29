package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNetworks handles GET /networks: the declared networks.
func (a *Api) GetNetworks(c echo.Context, params server.GetNetworksParams) error {
	return a.handleList(c, "GetNetworks", "network",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetNetworks(ctx, p)
		})
}

// PostNetworkSegments handles POST /networks/{net_id}/segments: declare an
// address range of a network, as the historical collector's
// rest_post_network_segments did. The caller's primary group becomes
// responsible for the segment.
func (a *Api) PostNetworkSegments(c echo.Context, netId string) error {
	log := echolog.GetLogHandler(c, "PostNetworkSegments")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireNetworkManager(c); err != nil {
		return err
	}
	netID, err := strconv.Atoi(netId)
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "network id must be an integer, got %q", netId)
	}

	var body server.PostNetworkSegmentsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	network, found, err := odb.GetNetworkRange(ctx, netID)
	if err != nil {
		log.Error("cannot read network", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read network")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "network %d not found", netID)
	}
	// Only the team responsible for the network carves it up, or a manager.
	if !IsManager(c) && !slices.Contains(UserGroupsFromContext(c), network.TeamResponsible) {
		return JSONProblemf(c, http.StatusForbidden, "not responsible for network %d", netID)
	}

	segType := "static"
	if body.SegType != nil && *body.SegType != "" {
		segType = string(*body.SegType)
	}
	if segType != "static" && segType != "dynamic" {
		return JSONProblemf(c, http.StatusBadRequest, "seg_type must be static or dynamic, got %q", segType)
	}

	// The overlap check compares addresses with INET_ATON: IPv4 only, like networks.
	netBegin, errB := netip.ParseAddr(network.Begin)
	netEnd, errE := netip.ParseAddr(network.End)
	if errB != nil || errE != nil {
		return JSONProblemf(c, http.StatusConflict, "network %d has no usable range", netID)
	}
	parse := func(name, value string) (netip.Addr, error) {
		addr, err := netip.ParseAddr(strings.TrimSpace(value))
		if err != nil || !addr.Is4() {
			return addr, fmt.Errorf("%s must be an IPv4 address, got %q", name, value)
		}
		if addr.Less(netBegin) || netEnd.Less(addr) {
			return addr, fmt.Errorf("%s %s is outside the network range %s - %s", name, addr, netBegin, netEnd)
		}
		return addr, nil
	}
	begin, err := parse("seg_begin", body.SegBegin)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	end, err := parse("seg_end", body.SegEnd)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if end.Less(begin) {
		return JSONProblemf(c, http.StatusBadRequest, "seg_end %s is before seg_begin %s", end, begin)
	}

	otherID, taken, err := odb.NetworkSegmentOverlapping(ctx, netID, begin.String(), end.String())
	if err != nil {
		log.Error("cannot check segment overlap", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check segment overlap")
	}
	if taken {
		return JSONProblemf(c, http.StatusConflict, "range %s - %s overlaps segment %d", begin, end, otherID)
	}

	var groupID *int64
	if userID := authUserID(c); userID != nil {
		id, found, err := odb.UserPrimaryGroupID(ctx, *userID)
		if err != nil {
			log.Error("cannot get primary group", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot get primary group")
		}
		if found {
			groupID = &id
		}
	}

	prefix := fmt.Sprintf("%s/%d", network.Network, network.Netmask)
	log.Info("called", "network", prefix, "begin", begin.String(), "end", end.String())

	id, err := odb.InsertNetworkSegment(ctx, netID, segType, begin.String(), end.String(), groupID)
	if err != nil {
		log.Error("cannot create segment", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create segment")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "networks.segment.create",
		User:   userEmail,
		Fmt:    "created %(type)s segment %(begin)s-%(end)s of network %(network)s",
		Dict:   map[string]any{"type": segType, "begin": begin.String(), "end": end.String(), "network": prefix},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	row, err := odb.GetNetworkSegmentRow(ctx, id)
	if err != nil || row == nil {
		log.Error("cannot read the created segment", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "segment %d created but cannot be read back", id)
	}
	return c.JSON(http.StatusOK, map[string]any{"data": []any{row}, "info": fmt.Sprintf("segment %d created", id)})
}
