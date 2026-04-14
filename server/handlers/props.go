package serverhandlers

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/opensvc/oc3/server"
)

func buildSelectClause(props []string, mapping propMapping) ([]string, error) {
	exprs := make([]string, 0, len(props))
	for _, prop := range props {
		if strings.Contains(prop, ".") {
			// Cross-table prop: use "table.column"
			exprs = append(exprs, prop)
			continue
		}
		if mapping.Props == nil {
			return nil, fmt.Errorf("no SQL expressions defined for this resource")
		}
		def, ok := mapping.Props[prop]
		if !ok {
			return nil, fmt.Errorf("no SQL expression for prop %q", prop)
		}
		expr := def.selectExpr()
		if expr == "" {
			return nil, fmt.Errorf("prop %q has no SQL expression or column reference", prop)
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

func availableProps(mapping propMapping) []string {
	props := make([]string, 0, len(mapping.Available))
	for _, prop := range mapping.Available {
		if _, blocked := mapping.Blacklist[prop]; !blocked {
			props = append(props, prop)
		}
	}
	return props
}

func defaultProps(mapping propMapping) []string {
	source := mapping.Available
	if mapping.Default != nil {
		source = mapping.Default
	}
	props := make([]string, 0, len(source))
	for _, prop := range source {
		if _, blocked := mapping.Blacklist[prop]; !blocked {
			props = append(props, prop)
		}
	}
	return props
}

func buildProps(props *server.InQueryProps, mapping propMapping) ([]string, error) {
	allowedSet := make(map[string]struct{}, len(mapping.Available))
	defaultProps := defaultProps(mapping)

	for _, prop := range mapping.Available {
		allowedSet[prop] = struct{}{}
	}

	if props == nil || strings.TrimSpace(string(*props)) == "" {
		return defaultProps, nil
	}

	selected := make([]string, 0, len(mapping.Available))
	seen := make(map[string]struct{}, len(mapping.Available))
	for _, rawProp := range strings.Split(string(*props), ",") {
		prop := strings.TrimSpace(rawProp)
		if prop == "" {
			continue
		}
		// Cross-table prop: "table.column"
		if table, col, ok := strings.Cut(prop, "."); ok {
			jd, joinKnown := mapping.Joins[table]
			if !joinKnown {
				return nil, fmt.Errorf("prop does not exist: %v", []string{table, col})
			}
			refMapping, refFound := propsMapping[jd.MappingKey]
			if !refFound {
				return nil, fmt.Errorf("prop does not exist: %v", []string{table, col})
			}
			colAllowed := false
			for _, c := range refMapping.Available {
				if c == col {
					colAllowed = true
					break
				}
			}
			if !colAllowed {
				return nil, fmt.Errorf("prop does not exist: %v", []string{table, col})
			}
			if _, done := seen[prop]; done {
				continue
			}
			seen[prop] = struct{}{}
			selected = append(selected, prop)
			continue
		}
		if _, ok := allowedSet[prop]; !ok {
			return nil, fmt.Errorf("unknown prop %q", prop)
		}
		if _, blocked := mapping.Blacklist[prop]; blocked {
			return nil, fmt.Errorf("prop %q is not allowed", prop)
		}
		if _, done := seen[prop]; done {
			continue
		}
		seen[prop] = struct{}{}
		selected = append(selected, prop)
	}

	if len(selected) == 0 {
		return defaultProps, nil
	}

	return selected, nil
}

func buildTypeHints(props []string, mapping propMapping) map[string]string {
	hints := make(map[string]string, len(props))
	for _, prop := range props {
		if table, col, ok := strings.Cut(prop, "."); ok {
			jd, joinKnown := mapping.Joins[table]
			if !joinKnown {
				continue
			}
			refMapping, refFound := propsMapping[jd.MappingKey]
			if !refFound {
				continue
			}
			if def, ok := refMapping.Props[col]; ok && def.Kind != "" {
				hints[prop] = def.Kind
			}
			continue
		}
		if def, ok := mapping.Props[prop]; ok && def.Kind != "" {
			hints[prop] = def.Kind
		}
	}
	return hints
}

func filterItemFields(v any, props []string) (map[string]any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var source map[string]any
	if err := json.Unmarshal(data, &source); err != nil {
		return nil, err
	}

	filteredItem := make(map[string]any, len(props))
	for _, prop := range props {
		if value, ok := source[prop]; ok {
			filteredItem[prop] = value
		}
	}

	return filteredItem, nil
}

func filterItemsFields[T any](items []T, props []string) ([]map[string]any, error) {
	filteredItems := make([]map[string]any, 0, len(items))
	for _, item := range items {
		v, err := filterItemFields(item, props)
		if err != nil {
			return nil, err
		}
		filteredItems = append(filteredItems, v)
	}
	return filteredItems, nil
}
