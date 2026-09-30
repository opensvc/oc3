package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

func IsNetworkManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "NetworkManager")
}

func requireNetworkManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsNetworkManager(c) {
		return denyRequest(c, http.StatusForbidden, "NetworkManager privilege required")
	}
	return nil
}

// PostNetworks handles POST /networks: declare a network, as the historical
// collector's rest_post_networks did.
func (a *Api) PostNetworks(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostNetworks")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireNetworkManager(c); err != nil {
		return err
	}

	var body server.PostNetworksJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	// The begin, end and broadcast columns are computed with inet_aton: IPv4 only.
	addr, err := netip.ParseAddr(strings.TrimSpace(body.Network))
	if err != nil || !addr.Is4() {
		return JSONProblemf(c, http.StatusBadRequest, "network must be an IPv4 address, got %q", body.Network)
	}
	if body.Netmask < 0 || body.Netmask > 32 {
		return JSONProblemf(c, http.StatusBadRequest, "netmask must be a prefix length between 0 and 32")
	}
	prefix := netip.PrefixFrom(addr, body.Netmask)
	if masked := prefix.Masked(); masked.Addr() != addr {
		return JSONProblemf(c, http.StatusBadRequest, "%s is not a network address, did you mean %s?", prefix, masked)
	}

	insert := cdb.NetworkInsert{Network: addr.String(), Netmask: body.Netmask}

	if body.Gateway != nil && strings.TrimSpace(*body.Gateway) != "" {
		gw, err := netip.ParseAddr(strings.TrimSpace(*body.Gateway))
		if err != nil || !gw.Is4() || !prefix.Contains(gw) {
			return JSONProblemf(c, http.StatusBadRequest, "gateway must be an IPv4 address inside %s", prefix)
		}
		s := gw.String()
		insert.Gateway = &s
	}
	if body.Prio != nil {
		if *body.Prio < 0 || *body.Prio > 99 {
			return JSONProblemf(c, http.StatusBadRequest, "prio must be between 0 and 99")
		}
		insert.Prio = *body.Prio
	}
	if body.Pvid != nil {
		if *body.Pvid < 0 || *body.Pvid > 4094 {
			return JSONProblemf(c, http.StatusBadRequest, "pvid must be a VLAN id between 0 and 4094")
		}
		insert.Pvid = body.Pvid
	}
	if body.Comment != nil && *body.Comment != "" {
		insert.Comment = body.Comment
	}

	if body.Name != nil && strings.TrimSpace(*body.Name) != "" {
		name := strings.TrimSpace(*body.Name)
		otherID, taken, err := odb.NetworkIDByName(ctx, name)
		if err != nil {
			log.Error("cannot check network name", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check network name")
		}
		if taken {
			return JSONProblemf(c, http.StatusConflict, "a network named %s already exists: %d", name, otherID)
		}
		insert.Name = &name
	}
	otherID, taken, err := odb.NetworkIDByAddress(ctx, insert.Network, insert.Netmask)
	if err != nil {
		log.Error("cannot check network address", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check network address")
	}
	if taken {
		return JSONProblemf(c, http.StatusConflict, "network %s is already declared: %d", prefix, otherID)
	}

	// The responsible team defaults to the caller's primary group.
	if body.TeamResponsible != nil && strings.TrimSpace(*body.TeamResponsible) != "" {
		team := strings.TrimSpace(*body.TeamResponsible)
		ok, err := odb.IsAssignableTeam(ctx, team)
		if err != nil {
			log.Error("cannot check team", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check team")
		}
		if !ok {
			return JSONProblemf(c, http.StatusBadRequest, "team_responsible must be an existing organisational group, got %q", team)
		}
		insert.TeamResponsible = &team
	} else if userID := authUserID(c); userID != nil {
		role, found, err := odb.UserPrimaryGroupRole(ctx, *userID)
		if err != nil {
			log.Error("cannot get primary group", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot get primary group")
		}
		if found {
			insert.TeamResponsible = &role
		}
	}

	log.Info("called", "network", prefix.String())

	id, err := odb.InsertNetwork(ctx, insert)
	if err != nil {
		log.Error("cannot create network", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create network")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	name := ""
	if insert.Name != nil {
		name = *insert.Name
	}
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "networks.create",
		User:   userEmail,
		Fmt:    "created network %(name)s %(network)s",
		Dict:   map[string]any{"name": name, "network": prefix.String()},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	row, err := odb.GetNetworkRow(ctx, id)
	if err != nil || row == nil {
		log.Error("cannot read the created network", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "network %d created but cannot be read back", id)
	}
	return c.JSON(http.StatusOK, map[string]any{"data": []any{row}, "info": fmt.Sprintf("network %d created", id)})
}
