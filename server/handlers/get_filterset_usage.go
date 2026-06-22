package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetFiltersetUsage handles GET /filtersets/{filterset_id}/usage
func (a *Api) GetFiltersetUsage(c echo.Context, filtersetId string) error {
	log := echolog.GetLogHandler(c, "GetFiltersetUsage")
	odb := a.ODB
	ctx := c.Request().Context()

	fsetID, _, err := odb.FiltersetByIDOrName(ctx, filtersetId)
	if err != nil {
		log.Error("cannot lookup filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup filterset %s", filtersetId)
	}
	if fsetID == 0 {
		return JSONProblemf(c, http.StatusNotFound, "fset %s does not exist", filtersetId)
	}

	fsets, err := odb.FiltersetUsageEncapFiltersets(ctx, fsetID)
	if err != nil {
		log.Error("cannot fetch encapsulating filtersets", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch filterset usage")
	}
	rulesets, err := odb.FiltersetUsageRulesets(ctx, fsetID)
	if err != nil {
		log.Error("cannot fetch rulesets", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch filterset usage")
	}
	thresholds, err := odb.FiltersetUsageThresholds(ctx, fsetID)
	if err != nil {
		log.Error("cannot fetch thresholds", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch filterset usage")
	}

	thresholdStrings := make([]string, 0, len(thresholds))
	for _, t := range thresholds {
		thresholdStrings = append(thresholdStrings, t.ChkType+"."+t.ChkInstance+":"+t.ChkLow+"-"+t.ChkHigh)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"data": map[string]any{
			"filtersets": fsets,
			"rulesets":   rulesets,
			"thresholds": thresholdStrings,
		},
	})
}
