package worker

import "fmt"

type (
	data map[string]any
)

func (d data) getString(key string) (s string, err error) {
	if i, ok := d[key]; !ok {
		err = fmt.Errorf("invalid schema: no key '%s'", key)
	} else if s, ok = i.(string); !ok {
		err = fmt.Errorf("invalid schema: %s not a string", key)
	} else if len(s) == 0 {
		err = fmt.Errorf("unexpected empty %s value", key)
	}
	return
}

func (d data) getDictSubKeys(key string) (l []string, err error) {
	m, ok := d[key].(map[string]any)
	if !ok {
		err = fmt.Errorf("invalid schema: no key '%s'", key)
		return
	}
	for s := range m {
		l = append(l, s)
	}
	return
}

func mapTo(m map[string]any, k ...string) (any, bool) {
	if len(k) <= 1 {
		v, ok := m[k[0]]
		return v, ok
	}
	if v, ok := m[k[0]].(map[string]any); !ok {
		return v, ok
	} else {
		return mapTo(v, k[1:]...)
	}
}

func mapToA(m map[string]any, defaultValue any, k ...string) any {
	if v, ok := mapTo(m, k...); ok && v != nil {
		return v
	} else {
		return defaultValue
	}
}

func mapToBool(m map[string]any, defaultValue bool, k ...string) bool {
	return mapToA(m, defaultValue, k...).(bool)
}

// mapToBoolS returns "T" or "F"
func mapToBoolS(m map[string]any, defaultValue bool, k ...string) string {
	if mapToBool(m, defaultValue, k...) {
		return "T"
	} else {
		return "F"
	}
}

func mapToS(m map[string]any, defaultValue string, k ...string) string {
	return mapToA(m, defaultValue, k...).(string)
}

func mapToInt(m map[string]any, defaultValue int, k ...string) int {
	a := mapToA(m, defaultValue, k...)
	switch v := a.(type) {
	case int:
		return v
	case float64:
		return int(v)
	default:
		return defaultValue
	}
}

func mapToMap(m map[string]any, defaultValue map[string]any, k ...string) map[string]any {
	return mapToA(m, defaultValue, k...).(map[string]any)
}
