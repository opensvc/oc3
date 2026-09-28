package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
)

// modulesetParent resolves the moduleset a composition change applies to, with
// its name, and checks the caller is a CompManager responsible for it.
func (a *Api) modulesetParent(c echo.Context, modsetID string) (int64, string, error) {
	ctx := c.Request().Context()
	id, err := a.resolveModuleset(ctx, modsetID)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireModulesetResponsible(ctx, c, id)
	}
	if err != nil {
		return 0, "", err
	}
	name, err := a.ODB.CompObjectName(ctx, cdb.CompModulesetKind, id)
	if err != nil {
		return 0, "", compDesignerError(c, "read the moduleset name", err)
	}
	return id, name, nil
}

// PostComplianceModulesetModuleset handles POST
// /compliance/modulesets/{modset_id}/modulesets/{child_modset_id}, as the
// historical attach_moduleset_to_moduleset(): by a CompManager responsible for
// the parent, a moduleset published to them becomes a child of another, unless
// that would close a loop.
func (a *Api) PostComplianceModulesetModuleset(c echo.Context, modsetId, childModsetId string) error {
	ctx := c.Request().Context()
	parent, parentName, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.publishedModuleset(c, childModsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if parent == child {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "abort action to avoid introducing a recursion loop"))
	}
	if attached, err := a.ODB.CompModulesetChildAttached(ctx, parent, child); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesetModuleset", err))
	} else if attached {
		return compInfo(c, "already attached")
	}
	if loop, err := a.ODB.CompModulesetLoop(ctx, child, parent); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesetModuleset", err))
	} else if loop {
		return httpProblem(c, httpErrorf(http.StatusConflict, "the parent moduleset is already a child of the encapsulated moduleset. abort encapsulation not to cause infinite recursion"))
	}
	childName, err := a.ODB.CompObjectName(ctx, cdb.CompModulesetKind, child)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesetModuleset", err))
	}
	if err := a.ODB.AttachCompModulesetChild(ctx, parent, child); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesetModuleset", err))
	}
	a.compLog(c, "compliance.moduleset.moduleset.attach", "attached moduleset %(child_modset_name)s to moduleset %(parent_modset_name)s",
		map[string]any{"child_modset_name": childName, "parent_modset_name": parentName}, "", "")
	a.afterRulesetChange(c, false)
	return compInfo(c, "moduleset attached")
}

// DeleteComplianceModulesetModuleset handles DELETE
// /compliance/modulesets/{modset_id}/modulesets/{child_modset_id}, as the
// historical detach_moduleset_from_moduleset(), by a CompManager responsible for
// the parent.
func (a *Api) DeleteComplianceModulesetModuleset(c echo.Context, modsetId, childModsetId string) error {
	ctx := c.Request().Context()
	parent, parentName, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.resolveModuleset(ctx, childModsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	childName, err := a.ODB.CompObjectName(ctx, cdb.CompModulesetKind, child)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceModulesetModuleset", err))
	}
	if err := a.ODB.DetachCompModulesetChild(ctx, parent, child); err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceModulesetModuleset", err))
	}
	a.compLog(c, "compliance.moduleset.detach", "detach moduleset %(child_modset_name)s from moduleset %(parent_modset_name)s",
		map[string]any{"child_modset_name": childName, "parent_modset_name": parentName}, "", "")
	a.afterRulesetChange(c, false)
	return compInfo(c, "moduleset detached")
}

// PostComplianceModulesetRuleset handles POST
// /compliance/modulesets/{modset_id}/rulesets/{rset_id}, as the historical
// attach_ruleset_to_moduleset(): by a CompManager responsible for the moduleset,
// a ruleset published to them adds its variables to the modules' environment.
func (a *Api) PostComplianceModulesetRuleset(c echo.Context, modsetId, rsetId string) error {
	ctx := c.Request().Context()
	modset, modsetName, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	rset, err := a.resolveRuleset(ctx, rsetId)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, rset)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	if attached, err := a.ODB.CompModulesetRulesetAttached(ctx, modset, rset); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesetRuleset", err))
	} else if attached {
		return compInfo(c, "already attached")
	}
	names, err := a.rulesetNames(c, rset)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.AttachCompModulesetRuleset(ctx, modset, rset); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesetRuleset", err))
	}
	a.compLog(c, "compliance.moduleset.ruleset.attach", "attach ruleset %(rset_name)s to moduleset %(modset_name)s",
		map[string]any{"rset_name": names[0], "modset_name": modsetName}, "", "")
	a.afterRulesetChange(c, false)
	return compInfo(c, "ruleset attached")
}

// DeleteComplianceModulesetRuleset handles DELETE
// /compliance/modulesets/{modset_id}/rulesets/{rset_id}, as the historical
// detach_ruleset_from_moduleset(): by a CompManager responsible for the
// moduleset, the ruleset being published to them.
func (a *Api) DeleteComplianceModulesetRuleset(c echo.Context, modsetId, rsetId string) error {
	ctx := c.Request().Context()
	modset, modsetName, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	rset, err := a.resolveRuleset(ctx, rsetId)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, rset)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	names, err := a.rulesetNames(c, rset)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.DetachCompModulesetRuleset(ctx, modset, rset); err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceModulesetRuleset", err))
	}
	a.compLog(c, "compliance.ruleset.detach", "detach ruleset %(rset_name)s from moduleset %(modset_name)s",
		map[string]any{"rset_name": names[0], "modset_name": modsetName}, "", "")
	a.afterRulesetChange(c, false)
	return compInfo(c, "ruleset detached")
}
