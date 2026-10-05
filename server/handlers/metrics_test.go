package serverhandlers

import (
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
)

func metricContext(body string) echo.Context {
	req := httptest.NewRequest("POST", "/metrics", strings.NewReader(body))
	return echo.New().NewContext(req, httptest.NewRecorder())
}

func TestReadMetricFields(t *testing.T) {
	f, err := readMetricFields(metricContext(`{"metric_name":"  disks  ","metric_sql":"select 1","metric_col_value_index":2,"metric_historize":"T"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *f.Name != "disks" || *f.SQL != "select 1" || *f.ValueIndex != 2 || *f.Historize != "T" || f.InstanceIndex != nil || f.ClearInstanceIndex {
		t.Errorf("unexpected fields: %+v", f)
	}

	// An absent instance index is left alone, a null one is cleared.
	f, _ = readMetricFields(metricContext(`{"metric_col_instance_label":"disk"}`))
	if f.ClearInstanceIndex || f.InstanceIndex != nil {
		t.Errorf("absent instance index: %+v", f)
	}
	f, _ = readMetricFields(metricContext(`{"metric_col_instance_index":null}`))
	if !f.ClearInstanceIndex {
		t.Errorf("null instance index not cleared: %+v", f)
	}
	f, _ = readMetricFields(metricContext(`{"metric_col_instance_index":1}`))
	if f.ClearInstanceIndex || f.InstanceIndex == nil || *f.InstanceIndex != 1 {
		t.Errorf("instance index: %+v", f)
	}

	for _, body := range []string{
		`{"metric_name":" "}`,
		`{"metric_name":"` + strings.Repeat("x", 101) + `"}`,
		`{"metric_col_value_index":-1}`,
		`{"metric_col_instance_index":-1}`,
		`{"metric_historize":"yes"}`,
		`not json`,
	} {
		if _, err := readMetricFields(metricContext(body)); err == nil {
			t.Errorf("%s: expected an error", body)
		}
	}
}

func TestMetricChanges(t *testing.T) {
	row := &cdb.MetricRow{
		Name: "disks", SQL: "select 1", ValueIndex: sql.NullInt64{Int64: 0, Valid: true},
		InstanceIndex: sql.NullInt64{Int64: 1, Valid: true}, InstanceLabel: "disk", Historize: "F",
	}
	name, sqlText, label, historize := "disks", "select 1", "disk", "F"
	zero, one := 0, 1
	same := cdb.MetricFields{Name: &name, SQL: &sqlText, ValueIndex: &zero, InstanceIndex: &one, InstanceLabel: &label, Historize: &historize}
	if changes := metricChanges(row, same); len(changes) != 0 {
		t.Errorf("same values: got changes %v", changes)
	}
	newName, newSQL, two, on := "disks2", "select 2", 2, "T"
	changes := metricChanges(row, cdb.MetricFields{Name: &newName, SQL: &newSQL, ValueIndex: &two, ClearInstanceIndex: true, Historize: &on})
	want := "metric_name: disks => disks2, metric_sql changed, metric_col_value_index: 0 => 2, metric_col_instance_index: 1 => , metric_historize: F => T"
	if got := strings.Join(changes, ", "); got != want {
		t.Errorf("changes:\n got %s\nwant %s", got, want)
	}
}
