package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// compTeamList answers with the publication or responsible groups of a
// compliance object; a non-manager sees only their own among them.
func (a *Api) compTeamList(c echo.Context, name string, k cdb.CompKind, objID int64, gtype string, params listEndpointParams) error {
	return a.handleList(c, name, "auth_group", params, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetCompTeamGroups(ctx, k, objID, gtype, p)
	})
}

// compTeamChange attaches or detaches a publication or responsible group of a
// compliance object, as attach_group_to_ruleset() and detach_group_from_ruleset()
// and their moduleset counterparts: by a CompManager responsible for the object.
// A non-manager may attach only one of their groups; Everybody may not be made
// responsible, nor be given the publication of a contextual ruleset.
func (a *Api) compTeamChange(c echo.Context, name string, k cdb.CompKind, objID int64, gtype, groupRef string, attach bool) error {
	ctx := c.Request().Context()
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if ok, err := a.ODB.CompObjectResponsible(ctx, k, objID, UserGroupsFromContext(c), IsManager(c)); err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	} else if !ok {
		return httpProblem(c, httpErrorf(http.StatusForbidden, "%s not found or not owned by you", k.Name))
	}
	group, found, err := a.ODB.AuthGroupByIDOrRole(ctx, groupRef)
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	if !found {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "group %s not found", groupRef))
	}
	objName, err := a.ODB.CompObjectName(ctx, k, objID)
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	dict := map[string]any{"gtype": gtype, "role": group.Role}
	nameKey := "rset_name"
	if k.Name == "moduleset" {
		nameKey = "modset_name"
	}
	dict[nameKey] = objName
	if !attach {
		if err := a.ODB.DetachCompTeam(ctx, k, objID, gtype, group.ID); err != nil {
			return httpProblem(c, compDesignerError(c, name, err))
		}
		a.compLog(c, "compliance."+k.Name+".detach", "detach %(gtype)s group %(role)s from "+k.Name+" %("+nameKey+")s", dict, "", "")
		a.afterRulesetChange(c, false)
		return compInfo(c, "group detached")
	}
	if group.Role == "Everybody" {
		if gtype == "responsible" {
			return httpProblem(c, httpErrorf(http.StatusBadRequest, "Giving responsibility of a %s to Everybody is not allowed", k.Name))
		}
		if k.Name == "ruleset" {
			if contextual, err := a.rulesetIsContextual(ctx, objID); err != nil {
				return httpProblem(c, compDesignerError(c, name, err))
			} else if contextual {
				return httpProblem(c, httpErrorf(http.StatusBadRequest, "Publishing a contextual ruleset to Everybody is not allowed"))
			}
		}
	}
	if !IsManager(c) && !HasGroup(c, group.Role) {
		return httpProblem(c, httpErrorf(http.StatusForbidden, "you can't attach a group you are not a member of"))
	}
	if attached, err := a.ODB.CompTeamAttached(ctx, k, objID, gtype, group.ID); err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	} else if attached {
		return compInfo(c, "%s group already attached", gtype)
	}
	if err := a.ODB.AttachCompTeam(ctx, k, objID, gtype, group.ID); err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	a.compLog(c, "compliance."+k.Name+".change", "attach %(gtype)s group %(role)s to "+k.Name+" %("+nameKey+")s", dict, "", "")
	a.afterRulesetChange(c, false)
	return compInfo(c, "group attached")
}

func (a *Api) rulesetIsContextual(ctx context.Context, id int64) (bool, error) {
	return a.ODB.RulesetIsContextual(ctx, id)
}

// compTeamBody reads the object and the group a bulk form names in its body.
func compTeamBody(c echo.Context, objKey string) (string, string, error) {
	entry, err := oneEntry(c)
	if err != nil {
		return "", "", err
	}
	obj, ok := entry[objKey]
	if !ok {
		return "", "", httpErrorf(http.StatusBadRequest, "The '%s' key is mandatory", objKey)
	}
	group, ok := entry["group_id"]
	if !ok {
		return "", "", httpErrorf(http.StatusBadRequest, "The 'group_id' key is mandatory")
	}
	return fmt.Sprint(obj), fmt.Sprint(group), nil
}

func listParams(props *server.InQueryProps, limit *server.InQueryLimit, offset *server.InQueryOffset, meta *server.InQueryMeta, stats *server.InQueryStats, orderby *server.InQueryOrderby, groupby *server.InQueryGroupby, filter *server.InQueryFilter) listEndpointParams {
	return listEndpointParams{props: props, limit: limit, offset: offset, meta: meta, stats: stats, orderby: orderby, groupby: groupby, filter: filter}
}

// GetComplianceRulesetPublications handles GET /compliance/rulesets/{rset_id}/publications.
func (a *Api) GetComplianceRulesetPublications(c echo.Context, rsetId string, params server.GetComplianceRulesetPublicationsParams) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamList(c, "GetComplianceRulesetPublications", cdb.CompRulesetKind, id, "publication",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetResponsibles handles GET /compliance/rulesets/{rset_id}/responsibles.
func (a *Api) GetComplianceRulesetResponsibles(c echo.Context, rsetId string, params server.GetComplianceRulesetResponsiblesParams) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamList(c, "GetComplianceRulesetResponsibles", cdb.CompRulesetKind, id, "responsible",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

func (a *Api) rulesetTeamChange(c echo.Context, name, rsetRef, groupRef, gtype string, attach bool) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetRef)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamChange(c, name, cdb.CompRulesetKind, id, gtype, groupRef, attach)
}

// PostComplianceRulesetPublication handles POST /compliance/rulesets/{rset_id}/publications/{group_id}.
func (a *Api) PostComplianceRulesetPublication(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "PostComplianceRulesetPublication", rsetId, groupId, "publication", true)
}

// DeleteComplianceRulesetPublication handles DELETE /compliance/rulesets/{rset_id}/publications/{group_id}.
func (a *Api) DeleteComplianceRulesetPublication(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "DeleteComplianceRulesetPublication", rsetId, groupId, "publication", false)
}

// PostComplianceRulesetResponsible handles POST /compliance/rulesets/{rset_id}/responsibles/{group_id}.
func (a *Api) PostComplianceRulesetResponsible(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "PostComplianceRulesetResponsible", rsetId, groupId, "responsible", true)
}

// DeleteComplianceRulesetResponsible handles DELETE /compliance/rulesets/{rset_id}/responsibles/{group_id}.
func (a *Api) DeleteComplianceRulesetResponsible(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "DeleteComplianceRulesetResponsible", rsetId, groupId, "responsible", false)
}

func (a *Api) rulesetsTeamBulk(c echo.Context, name, gtype string, attach bool) error {
	rset, group, err := compTeamBody(c, "ruleset_id")
	if err != nil {
		return httpProblem(c, err)
	}
	return a.rulesetTeamChange(c, name, rset, group, gtype, attach)
}

// PostComplianceRulesetsPublications handles POST /compliance/rulesets_publications.
func (a *Api) PostComplianceRulesetsPublications(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "PostComplianceRulesetsPublications", "publication", true)
}

// DeleteComplianceRulesetsPublications handles DELETE /compliance/rulesets_publications.
func (a *Api) DeleteComplianceRulesetsPublications(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "DeleteComplianceRulesetsPublications", "publication", false)
}

// PostComplianceRulesetsResponsibles handles POST /compliance/rulesets_responsibles.
func (a *Api) PostComplianceRulesetsResponsibles(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "PostComplianceRulesetsResponsibles", "responsible", true)
}

// DeleteComplianceRulesetsResponsibles handles DELETE /compliance/rulesets_responsibles.
func (a *Api) DeleteComplianceRulesetsResponsibles(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "DeleteComplianceRulesetsResponsibles", "responsible", false)
}
