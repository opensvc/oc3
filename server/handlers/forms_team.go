package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/xauth"
)

// formTeam describes one kind of form/group link: the publications, which make
// a form usable by a group, and the responsibles, who may change it. Each has
// the log actions and messages of the historical collector.
type formTeam struct {
	table      cdb.FormTeamTable
	addAction  string
	addFmt     string
	addedFmt   string
	delAction  string
	delFmt     string
	deletedFmt string
}

var (
	formPublicationTeam = formTeam{
		table:      cdb.FormPublications,
		addAction:  "form.publication.add",
		addFmt:     "Form %(form_id)s published to group %(role)s",
		addedFmt:   "Form %(form_id)s already published to group %(role)s",
		delAction:  "form.publication.delete",
		delFmt:     "Form %(form_id)s unpublished to group %(group_id)s",
		deletedFmt: "Form %(form_id)s already unpublished to group %(group_id)s",
	}
	formResponsibleTeam = formTeam{
		table:      cdb.FormResponsibles,
		addAction:  "form.responsible.add",
		addFmt:     "Form %(form_id)s responsibility to group %(role)s added",
		addedFmt:   "Form %(form_id)s responsibility to group %(role)s already added",
		delAction:  "form.responsible.delete",
		delFmt:     "Form %(form_id)s responsibility to group %(group_id)s removed",
		deletedFmt: "Form %(form_id)s responsibility to group %(group_id)s already removed",
	}
)

// GetFormPublications handles GET /forms/{form_id}/publications.
func (a *Api) GetFormPublications(c echo.Context, formId int, params server.GetFormPublicationsParams) error {
	return a.getFormTeam(c, "GetFormPublications", cdb.FormPublications, int64(formId), listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	})
}

// GetFormResponsibles handles GET /forms/{form_id}/responsibles.
func (a *Api) GetFormResponsibles(c echo.Context, formId int, params server.GetFormResponsiblesParams) error {
	return a.getFormTeam(c, "GetFormResponsibles", cdb.FormResponsibles, int64(formId), listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	})
}

// getFormTeam lists the groups linked to a form. Both lists require the form to
// be published to the caller, as form_published() does for both.
func (a *Api) getFormTeam(c echo.Context, handlerName string, table cdb.FormTeamTable, formID int64, p listEndpointParams) error {
	log := echolog.GetLogHandler(c, handlerName)
	if err := a.requireFormPublished(c, log, c.Request().Context(), formID); err != nil {
		return httpProblem(c, err)
	}
	return a.handleList(c, handlerName, "auth_group", p, func(ctx context.Context, lp cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFormTeam(ctx, table, formID, lp)
	})
}

// PostFormPublication handles POST /forms/{form_id}/publications/{group_id}.
func (a *Api) PostFormPublication(c echo.Context, formId int, groupId string) error {
	return a.formTeamOne(c, "PostFormPublication", formPublicationTeam, true, strconv.Itoa(formId), groupId)
}

// DeleteFormPublication handles DELETE /forms/{form_id}/publications/{group_id}.
func (a *Api) DeleteFormPublication(c echo.Context, formId int, groupId string) error {
	return a.formTeamOne(c, "DeleteFormPublication", formPublicationTeam, false, strconv.Itoa(formId), groupId)
}

// PostFormResponsible handles POST /forms/{form_id}/responsibles/{group_id}.
func (a *Api) PostFormResponsible(c echo.Context, formId int, groupId string) error {
	return a.formTeamOne(c, "PostFormResponsible", formResponsibleTeam, true, strconv.Itoa(formId), groupId)
}

// DeleteFormResponsible handles DELETE /forms/{form_id}/responsibles/{group_id}.
func (a *Api) DeleteFormResponsible(c echo.Context, formId int, groupId string) error {
	return a.formTeamOne(c, "DeleteFormResponsible", formResponsibleTeam, false, strconv.Itoa(formId), groupId)
}

// PostFormsPublications handles POST /forms_publications.
func (a *Api) PostFormsPublications(c echo.Context) error {
	return a.formTeamBulk(c, "PostFormsPublications", formPublicationTeam, true)
}

// DeleteFormsPublications handles DELETE /forms_publications.
func (a *Api) DeleteFormsPublications(c echo.Context) error {
	return a.formTeamBulk(c, "DeleteFormsPublications", formPublicationTeam, false)
}

// PostFormsResponsibles handles POST /forms_responsibles.
func (a *Api) PostFormsResponsibles(c echo.Context) error {
	return a.formTeamBulk(c, "PostFormsResponsibles", formResponsibleTeam, true)
}

// DeleteFormsResponsibles handles DELETE /forms_responsibles.
func (a *Api) DeleteFormsResponsibles(c echo.Context) error {
	return a.formTeamBulk(c, "DeleteFormsResponsibles", formResponsibleTeam, false)
}

func (a *Api) formTeamOne(c echo.Context, handlerName string, team formTeam, add bool, formID, groupID string) error {
	log := echolog.GetLogHandler(c, handlerName)
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	result, err := a.formTeamChange(ctx, c, log, team, add, formID, groupID)
	if err != nil {
		return httpProblem(c, err)
	}
	a.formNotify(ctx, log)
	return c.JSON(http.StatusOK, result)
}

// formTeamBulk takes the form and the group from the body keys form_id and
// group_id, one entry or a list, as the historical /forms_publications and
// /forms_responsibles handlers do.
func (a *Api) formTeamBulk(c echo.Context, handlerName string, team formTeam, add bool) error {
	log := echolog.GetLogHandler(c, handlerName)
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	entries, isList, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, err)
	}
	defer a.formNotify(ctx, log)
	return runEntries(c, entries, isList, func(entry map[string]any) (map[string]any, error) {
		formID, ok := entryString(entry, "form_id")
		if !ok {
			return nil, httpErrorf(http.StatusBadRequest, "The 'form_id' key is mandatory")
		}
		groupID, ok := entryString(entry, "group_id")
		if !ok {
			return nil, httpErrorf(http.StatusBadRequest, "The 'group_id' key is mandatory")
		}
		return a.formTeamChange(ctx, c, log, team, add, formID, groupID)
	})
}

// formTeamChange links or unlinks a form and a group. Adding resolves the group
// as lib_org_group() does, by id or role, among the caller's groups for a
// non-manager, and refuses privilege groups. An existing link, or a missing
// one, answers with an info message rather than an error.
func (a *Api) formTeamChange(ctx context.Context, c echo.Context, log *slog.Logger, team formTeam, add bool, formIDStr, groupIDStr string) (map[string]any, error) {
	if err := requireFormsManager(c); err != nil {
		return nil, err
	}
	formID, err := parseFormID(formIDStr)
	if err != nil {
		return nil, err
	}
	if err := a.requireFormResponsible(c, log, ctx, formID); err != nil {
		return nil, err
	}

	if !add {
		groupID, err := strconv.ParseInt(groupIDStr, 10, 64)
		if err != nil {
			return nil, httpErrorf(http.StatusBadRequest, "invalid group id %q", groupIDStr)
		}
		d := map[string]any{"form_id": formIDStr, "group_id": groupIDStr}
		exists, err := a.ODB.FormTeamExists(ctx, team.table, formID, groupID)
		if err != nil {
			return nil, httpInternal(log, "cannot check the form group link", err)
		}
		if !exists {
			return map[string]any{"info": pyFormat(team.deletedFmt, d)}, nil
		}
		if err := a.ODB.DeleteFormTeam(ctx, team.table, formID, groupID); err != nil {
			return nil, httpInternal(log, "cannot unlink the form and the group", err)
		}
		a.formLog(ctx, c, log, team.delAction, team.delFmt, d)
		return map[string]any{"info": pyFormat(team.delFmt, d)}, nil
	}

	var userGroupIDs []int64
	isManager := IsManager(c)
	if !isManager {
		user := UserInfoFromContext(c)
		if user == nil {
			return nil, httpErrorf(http.StatusUnauthorized, "missing user context")
		}
		userID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
		if err != nil {
			return nil, httpErrorf(http.StatusBadRequest, "invalid user id")
		}
		if userGroupIDs, err = a.ODB.UserGroupIDs(ctx, userID); err != nil {
			return nil, httpInternal(log, "cannot list user groups", err)
		}
	}
	group, status, err := a.ODB.OrgGroup(ctx, groupIDStr, userGroupIDs, isManager)
	if err != nil {
		return nil, httpInternal(log, "cannot resolve the group", err)
	}
	switch status {
	case cdb.OrgGroupNotFound:
		return nil, httpErrorf(http.StatusNotFound, "Group not found: %s", groupIDStr)
	case cdb.OrgGroupAmbiguous:
		return nil, httpErrorf(http.StatusBadRequest, "Ambiguous group id: %s", groupIDStr)
	case cdb.OrgGroupPrivileged:
		return nil, httpErrorf(http.StatusForbidden, "Operation not allowed on privilege group: %s", group.Role)
	}

	d := map[string]any{"form_id": formIDStr, "role": group.Role}
	exists, err := a.ODB.FormTeamExists(ctx, team.table, formID, group.ID)
	if err != nil {
		return nil, httpInternal(log, "cannot check the form group link", err)
	}
	if exists {
		return map[string]any{"info": pyFormat(team.addedFmt, d)}, nil
	}
	if err := a.ODB.InsertFormTeam(ctx, team.table, formID, group.ID); err != nil {
		return nil, httpInternal(log, "cannot link the form and the group", err)
	}
	a.formLog(ctx, c, log, team.addAction, team.addFmt, d)
	return map[string]any{"info": pyFormat(team.addFmt, d)}, nil
}
