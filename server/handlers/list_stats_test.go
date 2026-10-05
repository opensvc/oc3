package serverhandlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"testing"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/schema"
)

var statsTestMapping = propMapping{
	Props: map[string]propDef{
		"os_name": {Col: &schema.Col{T: schema.TNodes, Name: "os_name"}},
		"app":     {SQLExpr: "COALESCE(app, '') AS app", Kind: "string"},
	},
}

// statsFake answers the queries of listStats as the database would for rows,
// and records the params of each call.
type statsFake struct {
	rows  []map[string]any
	calls []cdb.ListParams
	// fail makes the grouped queries fail, as for a prop the database cannot
	// group on.
	fail bool
}

func (f *statsFake) fetch(_ context.Context, p cdb.ListParams) ([]map[string]any, error) {
	f.calls = append(f.calls, p)
	switch {
	case slices.Equal(p.SelectExprs, []string{"COUNT(*)"}):
		return []map[string]any{{"total": int64(len(f.rows))}}, nil
	case len(p.GroupBy) > 0 && f.fail:
		return nil, errors.New("cannot group")
	case len(p.GroupBy) > 0:
		prop := map[string]string{"nodes.os_name": "os_name", "COALESCE(app, '') AS app": "app"}[p.SelectExprs[0]]
		counts := map[any]int64{}
		var order []any
		for _, row := range f.rows {
			v := row[prop]
			if _, seen := counts[v]; !seen {
				order = append(order, v)
			}
			counts[v]++
		}
		if p.SelectExprs[1] == "COUNT(*) OVER ()" {
			return []map[string]any{{"value": order[0], "groups": int64(len(order))}}, nil
		}
		slices.SortStableFunc(order, func(a, b any) int { return int(counts[b] - counts[a]) })
		if p.Limit > 0 && len(order) > p.Limit {
			order = order[:p.Limit]
		}
		out := make([]map[string]any, 0, len(order))
		for _, v := range order {
			out = append(out, map[string]any{"value": v, "count": counts[v]})
		}
		return out, nil
	}
	out := make([]map[string]any, 0, len(f.rows))
	for _, row := range f.rows {
		item := map[string]any{}
		for _, prop := range p.Props {
			item[prop] = row[prop]
		}
		out = append(out, item)
	}
	return out, nil
}

func statsRows() []map[string]any {
	var rows []map[string]any
	add := func(n int, os any) {
		for range n {
			rows = append(rows, map[string]any{"os_name": os, "app": "QA"})
		}
	}
	add(5, "linux")
	add(3, "windows")
	add(2, "sunos")
	add(1, "")
	add(1, nil)
	return rows
}

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestListStatsByDatabase(t *testing.T) {
	f := &statsFake{rows: statsRows()}
	base := cdb.ListParams{Limit: 50, Offset: 100, OrderBy: []string{"nodes.nodename"}}
	got, err := listStats(context.Background(), quietLog, f.fetch, base, []string{"os_name"}, statsTestMapping, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"linux": 5, "windows": 3, "sunos": 2, "empty": 2}
	if !reflect.DeepEqual(got.values["os_name"], want) {
		t.Errorf("values: got %v, want %v", got.values["os_name"], want)
	}
	if got.total != 12 || got.other["os_name"] != 0 {
		t.Errorf("total %d other %d, want 12 and 0", got.total, got.other["os_name"])
	}
	// "" and NULL are two groups for the database, one value here.
	if got.distinct["os_name"] != 4 {
		t.Errorf("distinct: got %d, want 4", got.distinct["os_name"])
	}
	for _, p := range f.calls {
		if p.Offset != 0 {
			t.Errorf("the stats must ignore the pagination, got offset %d", p.Offset)
		}
	}
	grouped := f.calls[1]
	if !slices.Equal(grouped.GroupBy, []string{"1"}) || !slices.Equal(grouped.OrderBy, []string{"2 DESC", "1"}) || grouped.Limit != 0 {
		t.Errorf("grouped query: got group %v order %v limit %d", grouped.GroupBy, grouped.OrderBy, grouped.Limit)
	}
}

func TestListStatsLimit(t *testing.T) {
	f := &statsFake{rows: statsRows()}
	got, err := listStats(context.Background(), quietLog, f.fetch, cdb.ListParams{}, []string{"os_name"}, statsTestMapping, nil, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"linux": 5, "windows": 3}
	if !reflect.DeepEqual(got.values["os_name"], want) {
		t.Errorf("values: got %v, want %v", got.values["os_name"], want)
	}
	if got.other["os_name"] != 4 {
		t.Errorf("other: got %d, want 4", got.other["os_name"])
	}
	// Cut values: the distinct count comes from a count of the groups.
	if got.distinct["os_name"] != 5 {
		t.Errorf("distinct: got %d, want 5 groups", got.distinct["os_name"])
	}
}

func TestListStatsFallsBackToGo(t *testing.T) {
	for _, c := range []struct {
		name string
		fake *statsFake
		inGo bool
	}{
		{"the database cannot group", &statsFake{rows: statsRows(), fail: true}, false},
		{"the fetcher groups itself", &statsFake{rows: statsRows()}, true},
	} {
		got, err := listStats(context.Background(), quietLog, c.fake.fetch, cdb.ListParams{}, []string{"os_name"}, statsTestMapping, nil, 2, c.inGo)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		want := map[string]int{"linux": 5, "windows": 3}
		if !reflect.DeepEqual(got.values["os_name"], want) || got.other["os_name"] != 4 || got.distinct["os_name"] != 4 || got.total != 12 {
			t.Errorf("%s: got values %v other %d distinct %d total %d", c.name, got.values["os_name"], got.other["os_name"], got.distinct["os_name"], got.total)
		}
	}
}

func TestTopValues(t *testing.T) {
	values := map[string]int{"b": 2, "a": 2, "c": 5, "d": 1}
	if got := topValues(values, 2); !reflect.DeepEqual(got, map[string]int{"c": 5, "a": 2}) {
		t.Errorf("got %v: ties are broken by value", got)
	}
	if got := topValues(values, 0); len(got) != 4 {
		t.Errorf("no limit: got %v", got)
	}
}
