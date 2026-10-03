package serverhandlers

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"gopkg.in/yaml.v3"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	// metricSamplesLimit bounds the rows returned for a metric.
	metricSamplesLimit = 1000
	// metricSamplesTimeout bounds the time a metric request may run.
	metricSamplesTimeout = 10 * time.Second
)

// GetReportDefinition handles GET /reports/{report_id}/definition: the YAML of the
// report parsed, for the client to lay the report out, as the historical
// /reports/<id>/definition did.
func (a *Api) GetReportDefinition(c echo.Context, reportId string) error {
	log := echolog.GetLogHandler(c, "GetReportDefinition")
	ctx := c.Request().Context()
	definition, found, err := a.ODB.ReportVisible(ctx, reportId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot read report", "report_id", reportId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read report")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "report %s not found", reportId)
	}
	parsed := map[string]any{}
	if strings.TrimSpace(definition) != "" {
		if err := yaml.Unmarshal([]byte(definition), &parsed); err != nil {
			return JSONProblemf(c, http.StatusUnprocessableEntity, "the report definition is not valid YAML: %s", err)
		}
	}
	return c.JSON(http.StatusOK, map[string]any{"data": parsed})
}

// selectRequest tells a request that reads: the first word, past the comments,
// is SELECT or WITH. The transaction it runs in is read-only already; this keeps
// out the statements a read-only transaction lets through, such as DDL.
var selectRequest = regexp.MustCompile(`(?is)^\s*(?:(?:--[^\n]*\n|#[^\n]*\n|/\*.*?\*/)\s*)*(SELECT|WITH)\b`)

// GetMetricSamples handles GET /metrics/{metric_id}/samples: the result of the
// metric request, run now, for a caller who may see the metric. The placeholders of
// the request stand for the nodes and services the caller may see, those of their
// session filterset when they have one (MetricScope).
func (a *Api) GetMetricSamples(c echo.Context, metricId string) error {
	log := echolog.GetLogHandler(c, "GetMetricSamples")
	groups, isManager := UserGroupsFromContext(c), IsManager(c)
	ctx, cancel := context.WithTimeout(c.Request().Context(), metricSamplesTimeout)
	defer cancel()

	request, found, err := a.ODB.MetricVisible(ctx, metricId, groups, isManager)
	if err != nil {
		log.Error("cannot read metric", "metric_id", metricId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read metric")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "metric %s not found", metricId)
	}
	if strings.TrimSpace(request) == "" {
		return c.JSON(http.StatusOK, server.MetricSamplesResponse{Data: server.MetricSamples{Columns: []string{}, Rows: [][]interface{}{}}})
	}
	if !selectRequest.MatchString(request) {
		return JSONProblemf(c, http.StatusUnprocessableEntity, "the metric request is not a SELECT")
	}
	if strings.Contains(request, "%%fset_node_ids%%") || strings.Contains(request, "%%fset_svc_ids%%") {
		sessionNodes, sessionSvcs, err := a.sessionScope(c)
		if err != nil {
			log.Error("cannot resolve the session filterset", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve the session filterset")
		}
		nodes, services, err := a.ODB.MetricScope(ctx, groups, isManager, sessionNodes, sessionSvcs)
		if err != nil {
			log.Error("cannot compute the metric scope", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot compute the metric scope")
		}
		request = strings.NewReplacer("%%fset_node_ids%%", nodes, "%%fset_svc_ids%%", services).Replace(request)
	}

	samples, err := a.ODB.RunMetricRequest(ctx, request, metricSamplesLimit)
	if err != nil {
		// The request is the metric author's: its error is theirs to read.
		log.Info("metric request failed", "metric_id", metricId, logkey.Error, err)
		return JSONProblemf(c, http.StatusUnprocessableEntity, "the metric request failed: %s", err)
	}
	rows := make([][]interface{}, len(samples.Rows))
	for i, row := range samples.Rows {
		rows[i] = row
	}
	return c.JSON(http.StatusOK, server.MetricSamplesResponse{Data: server.MetricSamples{
		Columns: samples.Columns, Rows: rows, Truncated: &samples.Truncated,
	}})
}
