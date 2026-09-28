package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// resolveModuleset returns the id of a moduleset given by id or by name: 404 when
// there is none.
func (a *Api) resolveModuleset(ctx context.Context, idOrName string) (int64, error) {
	id, found, err := a.ODB.CompModulesetID(ctx, idOrName)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, httpErrorf(http.StatusNotFound, "moduleset %s not found", idOrName)
	}
	return id, nil
}

// requireModulesetResponsible refuses a caller none of whose groups is
// responsible for the moduleset.
func (a *Api) requireModulesetResponsible(ctx context.Context, c echo.Context, id int64) error {
	ok, err := a.ODB.CompObjectResponsible(ctx, cdb.CompModulesetKind, id, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return compDesignerError(c, "check the moduleset responsibility", err)
	}
	if !ok {
		return httpErrorf(http.StatusForbidden, "you are not responsible for this moduleset")
	}
	return nil
}

// requireModulesetPublished refuses a caller the moduleset is not published to.
func (a *Api) requireModulesetPublished(ctx context.Context, c echo.Context, id int64) error {
	ok, err := a.ODB.CompObjectPublished(ctx, cdb.CompModulesetKind, id, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return compDesignerError(c, "check the moduleset publication", err)
	}
	if !ok {
		return httpErrorf(http.StatusForbidden, "you are not member of one of the moduleset publication groups")
	}
	return nil
}

// publishedModuleset resolves a moduleset the caller may read.
func (a *Api) publishedModuleset(c echo.Context, modsetID string) (int64, error) {
	ctx := c.Request().Context()
	id, err := a.resolveModuleset(ctx, modsetID)
	if err == nil {
		err = a.requireModulesetPublished(ctx, c, id)
	}
	return id, err
}

// GetComplianceModulesets handles GET /compliance/modulesets, as the historical
// rest_get_compliance_modulesets: the modulesets published to the caller's groups.
func (a *Api) GetComplianceModulesets(c echo.Context, params server.GetComplianceModulesetsParams) error {
	return a.handleList(c, "GetComplianceModulesets", "moduleset",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetComplianceModulesets(ctx, nil, p)
		})
}

// GetComplianceModuleset handles GET /compliance/modulesets/{modset_id}.
func (a *Api) GetComplianceModuleset(c echo.Context, modsetId string, params server.GetComplianceModulesetParams) error {
	id, err := a.resolveModuleset(c.Request().Context(), modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.modulesetResponse(c, id, params.Props)
}

func (a *Api) modulesetResponse(c echo.Context, id int64, props *server.InQueryProps) error {
	return a.handleItem(c, "GetComplianceModuleset", "moduleset", "id", strconv.FormatInt(id, 10), listEndpointParams{
		props: props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceModulesets(ctx, &id, p)
	})
}

// PostComplianceModulesets handles POST /compliance/modulesets, as the historical
// create_moduleset(): a moduleset named modset_name, authored by the caller,
// published to and under the responsibility of their default group.
func (a *Api) PostComplianceModulesets(c echo.Context) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	name, err := compString("modset_name", entry["modset_name"], true)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if _, found, err := a.ODB.CompModulesetID(ctx, name); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesets", err))
	} else if found {
		return httpProblem(c, httpErrorf(http.StatusConflict, "a moduleset named '%s' already exists", name))
	}
	caller, err := a.formCaller(ctx, c)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesets", err))
	}
	groupID, err := a.ODB.DefaultGroupOrManager(ctx, authUserID(c))
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesets", err))
	}
	id, err := a.ODB.CreateCompModuleset(ctx, name, caller.name, groupID)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModulesets", err))
	}
	a.afterRulesetChange(c, false)
	a.compLog(c, "compliance.moduleset.add", "added moduleset %(modset_name)s", map[string]any{"modset_name": name}, "", "")
	return a.modulesetResponse(c, id, nil)
}

// PostComplianceModuleset handles POST /compliance/modulesets/{modset_id}, as the
// historical rest_post_compliance_moduleset: a renaming, by a CompManager
// responsible for the moduleset, recording the update date.
func (a *Api) PostComplianceModuleset(c echo.Context, modsetId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	id, err := a.resolveModuleset(ctx, modsetId)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireModulesetResponsible(ctx, c, id)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	var name string
	for k, v := range entry {
		switch k {
		case "modset_name":
			if name, err = compString(k, v, true); err != nil {
				return httpProblem(c, err)
			}
		case "id", "modset_author", "modset_updated":
		default:
			return httpProblem(c, httpErrorf(http.StatusBadRequest, "unknown moduleset property %s", k))
		}
	}
	if name == "" {
		return compInfo(c, "No fields to update")
	}
	if other, found, err := a.ODB.CompModulesetID(ctx, name); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModuleset", err))
	} else if found && other != id {
		return httpProblem(c, httpErrorf(http.StatusConflict, "a moduleset named '%s' already exists", name))
	}
	if err := a.ODB.RenameCompModuleset(ctx, id, name); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceModuleset", err))
	}
	a.afterRulesetChange(c, false)
	a.compLog(c, "compliance.moduleset.change", "update properties %(data)s", map[string]any{"data": compJSON(map[string]any{"modset_name": name})}, "", "")
	return a.modulesetResponse(c, id, nil)
}

// PutComplianceModuleset handles PUT /compliance/modulesets/{modset_id}, the
// special actions on a moduleset, as the historical rest_put_compliance_moduleset:
// clone.
func (a *Api) PutComplianceModuleset(c echo.Context, modsetId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	id, err := a.resolveModuleset(ctx, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if action, _ := entry["action"].(string); action != "clone" {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "unsupported action %q", action))
	}
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	caller, err := a.formCaller(ctx, c)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PutComplianceModuleset", err))
	}
	groupID, err := a.ODB.DefaultGroupOrManager(ctx, authUserID(c))
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PutComplianceModuleset", err))
	}
	name, err := a.ODB.CompObjectName(ctx, cdb.CompModulesetKind, id)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PutComplianceModuleset", err))
	}
	_, cloneName, err := a.ODB.CloneCompModuleset(ctx, id, caller.name, groupID)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "PutComplianceModuleset", err))
	}
	a.afterRulesetChange(c, false)
	a.compLog(c, "compliance.moduleset.clone", "cloned moduleset %(o)s from %(n)s", map[string]any{"n": name, "o": cloneName}, "", "")
	return compInfo(c, "clone done. new moduleset name %s", cloneName)
}

// DeleteComplianceModuleset handles DELETE /compliance/modulesets/{modset_id}.
func (a *Api) DeleteComplianceModuleset(c echo.Context, modsetId string) error {
	id, err := a.resolveModuleset(c.Request().Context(), modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.deleteModuleset(c, id)
}

// DeleteComplianceModulesets handles DELETE /compliance/modulesets, the bulk
// form naming the moduleset by the id key of the body.
func (a *Api) DeleteComplianceModulesets(c echo.Context) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	raw, ok := entry["id"]
	if !ok {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "The 'id' key is mandatory"))
	}
	id, err := a.resolveModuleset(c.Request().Context(), fmt.Sprint(raw))
	if err != nil {
		return httpProblem(c, err)
	}
	return a.deleteModuleset(c, id)
}

// deleteModuleset deletes a moduleset and all its relations, as
// delete_moduleset(): by a CompManager whose group is responsible for it.
func (a *Api) deleteModuleset(c echo.Context, id int64) error {
	ctx := c.Request().Context()
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if err := a.requireModulesetResponsible(ctx, c, id); err != nil {
		return httpProblem(c, err)
	}
	name, err := a.ODB.CompObjectName(ctx, cdb.CompModulesetKind, id)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceModuleset", err))
	}
	if err := a.ODB.DeleteCompModuleset(ctx, id); err != nil {
		return httpProblem(c, compDesignerError(c, "DeleteComplianceModuleset", err))
	}
	a.afterRulesetChange(c, false)
	a.compLog(c, "compliance.moduleset.delete", "deleted moduleset %(modset_name)s", map[string]any{"modset_name": name}, "", "")
	return compInfo(c, "Moduleset %d deleted", id)
}

// GetComplianceModulesetUsage handles GET /compliance/modulesets/{modset_id}/usage,
// as the historical rest_get_compliance_moduleset_usage: the modulesets holding
// it as a child.
func (a *Api) GetComplianceModulesetUsage(c echo.Context, modsetId string) error {
	id, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	usage, err := a.ODB.CompModulesetUsage(c.Request().Context(), id)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceModulesetUsage", err))
	}
	return c.JSON(http.StatusOK, map[string]any{"data": usage})
}

// GetComplianceModulesetAmIResponsible handles GET
// /compliance/modulesets/{modset_id}/am_i_responsible.
func (a *Api) GetComplianceModulesetAmIResponsible(c echo.Context, modsetId string) error {
	ctx := c.Request().Context()
	id, found, err := a.ODB.CompModulesetID(ctx, modsetId)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceModulesetAmIResponsible", err))
	}
	ok := false
	if found {
		if ok, err = a.ODB.CompObjectResponsible(ctx, cdb.CompModulesetKind, id, UserGroupsFromContext(c), IsManager(c)); err != nil {
			return httpProblem(c, compDesignerError(c, "GetComplianceModulesetAmIResponsible", err))
		}
	}
	return c.JSON(http.StatusOK, map[string]bool{"data": ok})
}
