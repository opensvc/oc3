package serverhandlers

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	chartSamplesDefaultDays = 365
	chartSamplesMaxDays     = 5 * 365
)

// chartDefinition is the part of a chart definition the samples need: the
// historized metrics it draws and its options, in the historical format.
type chartDefinition struct {
	Metrics []struct {
		MetricID any    `yaml:"metric_id"`
		Label    string `yaml:"label"`
		Unit     string `yaml:"unit"`
	} `yaml:"Metrics"`
	Options struct {
		Stack bool `yaml:"stack"`
	} `yaml:"Options"`
}

// statsDirectory is where the scheduler writes the series of the historized
// metrics: the server reads them from the uploads directory it shares with it.
func statsDirectory() string {
	dir := viper.GetString("server.directories.uploads")
	if dir == "" {
		dir = viper.GetString("scheduler.directories.uploads")
	}
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "stats")
}

// readSeries reads the points of a whisper file between from and until, skipping
// the slots without a value.
func readSeries(path string, from, until int) ([][]float64, error) {
	wsp, err := whisper.Open(path)
	if err != nil {
		return nil, err
	}
	defer wsp.Close()
	ts, err := wsp.Fetch(from, until)
	if err != nil {
		return nil, err
	}
	points := [][]float64{}
	if ts == nil {
		return points, nil
	}
	for _, p := range ts.Points() {
		if math.IsNaN(p.Value) {
			continue
		}
		points = append(points, []float64{float64(p.Time), p.Value})
	}
	return points, nil
}

// GetChartSamples handles GET /charts/{chart_id}/samples: the series of the
// historized metrics a chart draws, for a caller who may see the chart, as the
// historical /reports/charts/<id>/samples did. A metric with instances gives one
// series per instance. The historical collector read the series of the session's
// filterset; oc3 has none, so the series read are those computed without one.
func (a *Api) GetChartSamples(c echo.Context, chartId string, params server.GetChartSamplesParams) error {
	log := echolog.GetLogHandler(c, "GetChartSamples")
	ctx := c.Request().Context()
	definitionText, found, err := a.ODB.ChartVisible(ctx, chartId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot read chart", "chart_id", chartId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read chart")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "chart %s not found", chartId)
	}
	var definition chartDefinition
	if err := yaml.Unmarshal([]byte(definitionText), &definition); err != nil {
		return JSONProblemf(c, http.StatusUnprocessableEntity, "the chart definition is not valid YAML: %s", err)
	}
	days := chartSamplesDefaultDays
	if params.Days != nil {
		days = min(max(*params.Days, 1), chartSamplesMaxDays)
	}
	stats := statsDirectory()
	if stats == "" {
		log.Error("no uploads directory configured")
		return JSONProblemf(c, http.StatusInternalServerError, "the series directory is not configured")
	}
	until := int(time.Now().Unix())
	from := until - days*24*3600

	series := []server.ChartSeries{}
	for _, m := range definition.Metrics {
		// The id names a directory: an integer only.
		metricID, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(m.MetricID)))
		if err != nil || metricID <= 0 {
			continue
		}
		files, err := filepath.Glob(filepath.Join(stats, "metrics", strconv.Itoa(metricID), "fsets", "0", "*.wsp"))
		if err != nil {
			continue
		}
		sort.Strings(files)
		for _, file := range files {
			points, err := readSeries(file, from, until)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Error("cannot read series", "file", file, logkey.Error, err)
				}
				continue
			}
			var instance *string
			if name := strings.TrimSuffix(filepath.Base(file), ".wsp"); name != "None" {
				instance = &name
			}
			label, unit := m.Label, m.Unit
			series = append(series, server.ChartSeries{
				MetricId: metricID, Label: &label, Unit: &unit, Instance: instance, Points: points,
			})
		}
	}
	return c.JSON(http.StatusOK, server.ChartSamplesResponse{Data: server.ChartSamples{
		Stack: definition.Options.Stack, Series: series,
	}})
}
