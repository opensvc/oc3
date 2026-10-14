package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// The answer of GET /groups/{group_id}/hidden_menu_entries/effect, the shape of
// the GroupHiddenMenuEffect schema.
type (
	menuEffectGroup struct {
		GroupID  int64  `json:"group_id"`
		Role     string `json:"role"`
		Personal bool   `json:"personal"`
	}
	menuEffectMember struct {
		UserID int64             `json:"user_id"`
		Name   string            `json:"name"`
		Email  string            `json:"email"`
		Hidden bool              `json:"hidden"`
		KeptBy []menuEffectGroup `json:"kept_by"`
	}
	menuEffectEntry struct {
		MenuEntry string             `json:"menu_entry"`
		Members   []menuEffectMember `json:"members"`
	}
	menuEffect struct {
		Members int               `json:"members"`
		Entries []menuEffectEntry `json:"entries"`
	}
)

// GetGroupHiddenMenuEffect handles GET /groups/{group_id}/hidden_menu_entries/effect:
// what the entries the group hides really hide to its members (see
// cdb.GroupHiddenMenuEffect). Visible as the hidden entries of the group are:
// to a Manager, or to a member.
func (a *Api) GetGroupHiddenMenuEffect(c echo.Context, groupId string) error {
	log := echolog.GetLogHandler(c, "GetGroupHiddenMenuEffect")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	group, found, err := a.ODB.AuthGroupByIDOrRole(ctx, groupId)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "group %s not found", groupId)
	}
	if group.Privilege {
		return JSONProblemf(c, http.StatusBadRequest, "a privilege group hides no menu entry")
	}
	if !IsManager(c) && !HasGroup(c, group.Role) {
		return JSONProblemf(c, http.StatusForbidden, "only a Manager or a member of %s sees the effect of its hidden menu entries", group.Role)
	}
	entries, members, err := a.ODB.GroupHiddenMenuEffect(ctx, group.ID)
	if err != nil {
		log.Error("cannot compute the effect of the hidden menu entries", "group_id", group.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot compute the effect of the hidden menu entries")
	}
	resp := menuEffect{Members: members, Entries: make([]menuEffectEntry, 0, len(entries))}
	for _, e := range entries {
		re := menuEffectEntry{MenuEntry: e.MenuEntry, Members: make([]menuEffectMember, 0, len(e.Members))}
		for _, m := range e.Members {
			rm := menuEffectMember{UserID: m.UserID, Name: m.Name, Email: m.Email, Hidden: m.Hidden, KeptBy: make([]menuEffectGroup, 0, len(m.KeptBy))}
			for _, g := range m.KeptBy {
				rm.KeptBy = append(rm.KeptBy, menuEffectGroup{GroupID: g.ID, Role: g.Role, Personal: g.Personal})
			}
			re.Members = append(re.Members, rm)
		}
		resp.Entries = append(resp.Entries, re)
	}
	return c.JSON(http.StatusOK, resp)
}
