package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// PostComplianceRulesetRuleset handles POST
// /compliance/rulesets/{rset_id}/rulesets/{child_rset_id}, as the historical
// attach_ruleset_to_ruleset(): by a CompManager responsible for the parent, a
// ruleset becomes a child of another, unless that would close a loop.
func (a *Api) PostComplianceRulesetRuleset(c echo.Context, rsetId, childRsetId string) error {
	ctx := c.Request().Context()
	parent, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.resolveRuleset(ctx, childRsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if parent == child {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "abort action to avoid introducing a recursion loop"))
	}
	if err := a.requireRulesetResponsible(ctx, c, parent); err != nil {
		return httpProblem(c, err)
	}
	if attached, err := a.ODB.CompRulesetChildAttached(ctx, parent, child); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesetRuleset", err))
	} else if attached {
		return httpProblem(c, httpErrorf(http.StatusConflict, "ruleset already attached"))
	}
	if loop, err := a.ODB.CompRulesetLoop(ctx, child, parent); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesetRuleset", err))
	} else if loop {
		return httpProblem(c, httpErrorf(http.StatusConflict, "the parent ruleset is already a child of the encapsulated ruleset. abort encapsulation not to cause infinite recursion"))
	}
	names, err := a.rulesetNames(c, parent, child)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.AttachCompRulesetChild(ctx, parent, child); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesetRuleset", err))
	}
	a.afterRulesetChange(c, true)
	a.compLog(c, "compliance.ruleset.attach", "attach ruleset %(rset_name)s to %(dst_rset_name)s",
		map[string]any{"rset_name": names[1], "dst_rset_name": names[0]}, "", "")
	return compInfo(c, "ruleset attached")
}

// DeleteComplianceRulesetRuleset handles DELETE
// /compliance/rulesets/{rset_id}/rulesets/{child_rset_id}, as the historical
// detach_ruleset_from_ruleset(): by a CompManager responsible for the parent, the
// child being published to them.
func (a *Api) DeleteComplianceRulesetRuleset(c echo.Context, rsetId, childRsetId string) error {
	ctx := c.Request().Context()
	parent, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.resolveRuleset(ctx, childRsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if err := a.requireRulesetResponsible(ctx, c, parent); err != nil {
		return httpProblem(c, err)
	}
	if err := a.requireRulesetPublished(ctx, c, child); err != nil {
		return httpProblem(c, err)
	}
	names, err := a.rulesetNames(c, parent, child)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.DetachCompRulesetChild(ctx, parent, child); err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceRulesetRuleset", err))
	}
	a.afterRulesetChange(c, true)
	a.compLog(c, "compliance.ruleset.detach", "detach ruleset %(rset_name)s from %(parent_rset_name)s",
		map[string]any{"rset_name": names[1], "parent_rset_name": names[0]}, "", "")
	return compInfo(c, "ruleset detached")
}

// rulesetNames returns the names of rulesets, for the logs.
func (a *Api) rulesetNames(c echo.Context, ids ...int64) ([]string, error) {
	names := make([]string, len(ids))
	for i, id := range ids {
		name, err := a.ODB.CompRulesetNameByID(c.Request().Context(), id)
		if err != nil {
			return nil, compDesignerError(c, "read the ruleset name", err)
		}
		names[i] = name
	}
	return names, nil
}

// PostComplianceRulesetFilterset handles POST
// /compliance/rulesets/{rset_id}/filtersets/{fset_id}, as the historical
// attach_filterset_to_ruleset(): the filterset selecting the nodes and services a
// contextual ruleset applies to, replacing the previous one.
func (a *Api) PostComplianceRulesetFilterset(c echo.Context, rsetId, fsetId string) error {
	ctx := c.Request().Context()
	rid, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	fid, fname, err := a.ODB.FiltersetByIDOrName(ctx, fsetId)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesetFilterset", err))
	}
	if fid == 0 {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "filterset %s not found", fsetId))
	}
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if err := a.requireRulesetResponsible(ctx, c, rid); err != nil {
		return httpProblem(c, err)
	}
	if current, _, found, err := a.ODB.CompRulesetFilterset(ctx, rid); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesetFilterset", err))
	} else if found && current == int64(fid) {
		return compInfo(c, "filterset attached")
	}
	names, err := a.rulesetNames(c, rid)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.SetCompRulesetFilterset(ctx, rid, int64(fid)); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesetFilterset", err))
	}
	a.afterRulesetChange(c, false)
	a.compLog(c, "compliance.ruleset.change", "attach filterset %(fset_name)s to ruleset %(rset_name)s",
		map[string]any{"rset_name": names[0], "fset_name": fname}, "", "")
	return compInfo(c, "filterset attached")
}

// DeleteComplianceRulesetFilterset handles DELETE
// /compliance/rulesets/{rset_id}/filtersets/{fset_id}, as the historical
// detach_filterset_from_ruleset(): the ruleset's filterset is removed, whichever
// it is.
func (a *Api) DeleteComplianceRulesetFilterset(c echo.Context, rsetId, fsetId string) error {
	ctx := c.Request().Context()
	rid, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if err := a.requireRulesetResponsible(ctx, c, rid); err != nil {
		return httpProblem(c, err)
	}
	_, fname, found, err := a.ODB.CompRulesetFilterset(ctx, rid)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceRulesetFilterset", err))
	}
	if !found {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "filterset not found"))
	}
	names, err := a.rulesetNames(c, rid)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.DeleteCompRulesetFilterset(ctx, rid); err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceRulesetFilterset", err))
	}
	a.afterRulesetChange(c, false)
	a.compLog(c, "compliance.filterset.detach", "detach filterset %(fset_name)s from ruleset %(rset_name)s",
		map[string]any{"rset_name": names[0], "fset_name": fname}, "", "")
	return compInfo(c, "filterset detached")
}
