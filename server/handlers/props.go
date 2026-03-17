package serverhandlers

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/opensvc/oc3/server"
)

func buildProps(props *server.InQueryProps, mapping propMapping) ([]string, error) {
	allowedSet := make(map[string]struct{}, len(mapping.Available))
	defaultProps := make([]string, 0, len(mapping.Available))

	for _, prop := range mapping.Available {
		allowedSet[prop] = struct{}{}
		if _, blocked := mapping.Blacklist[prop]; !blocked {
			defaultProps = append(defaultProps, prop)
		}
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
