package worker

import (
	"fmt"
)

type (
	daemonDataV2 struct {
		data data
	}

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

func (d *daemonDataV2) nodeNames() (l []string, err error) {
	return d.data.getDictSubKeys("nodes")
}

func (d *daemonDataV2) objectNames() (l []string, err error) {
	var keys []string
	keys, err = d.data.getDictSubKeys("services")
	if err != nil {
		return
	}
	for _, s := range keys {
		if s == "cluster" {
			continue
		}
		l = append(l, s)
	}
	return
}

func (d *daemonDataV2) clusterID() (s string, err error) {
	return d.data.getString("cluster_id")
}

func (d *daemonDataV2) clusterName() (s string, err error) {
	return d.data.getString("cluster_name")
}

func (d *daemonDataV2) parseNodeFrozen(i any) string {
	switch v := i.(type) {
	case int:
		if v > 0 {
			return "T"
		}
	case float64:
		if v > 0 {
			return "T"
		}
	}
	return "F"
}

func (d *daemonDataV2) nodeFrozen(nodename string) (s string, err error) {
	i, ok := mapTo(d.data, "nodes", nodename, "frozen")
	if !ok {
		err = fmt.Errorf("can't retrieve frozen for %s", nodename)
		return
	} else {
		return d.parseNodeFrozen(i), nil
	}
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

func (d *daemonDataV2) getFromKeys(keys ...string) (v any, err error) {
	if v, ok := mapTo(d.data, keys...); !ok {
		return v, fmt.Errorf("getFromKeys can't expand from %v", keys)
	} else {
		return v, nil
	}
}

// appFromObjectName returns object app value from nodes object instances status
func (d *daemonDataV2) appFromObjectName(svcname string, nodes ...string) string {
	for _, nodename := range nodes {
		if a, ok := mapTo(d.data, "nodes", nodename, "services", "status", svcname, "app"); ok {
			if app, ok := a.(string); ok {
				return app
			}
		}
	}
	return ""
}

func (d *daemonDataV2) objectStatus(objectName string) *DBObjStatus {
	if i, ok := mapTo(d.data, "services", objectName); ok && i != nil {
		if o, ok := i.(map[string]any); ok {
			oStatus := &DBObjStatus{
				availStatus: "n/a",
				status:      "n/a",
				placement:   "n/a",
				frozen:      "n/a",
				provisioned: "n/a",
			}
			if s, ok := o["avail"].(string); ok {
				oStatus.availStatus = s
			}
			if s, ok := o["overall"].(string); ok {
				oStatus.status = s
			}
			if s, ok := o["placement"].(string); ok {
				oStatus.placement = s
			}
			if s, ok := o["frozen"].(string); ok {
				oStatus.frozen = s
			}
			if prov, ok := o["provisioned"].(bool); ok {
				if prov {
					oStatus.provisioned = "True"
				} else {
					oStatus.provisioned = "False"
				}
			}
			return oStatus
		}
	}
	return nil
}

func (d *daemonDataV2) instanceStatusData(objectName string, nodename string) map[string]any {
	return mapToMap(d.data, "nodes", nodename, "services", "status", objectName)
}

func mapToA(m map[string]any, defaultValue any, k ...string) any {
	if v, ok := mapTo(m, k...); ok {
		return v
	} else {
		return defaultValue
	}
}

func mapToMap(m map[string]any, defaultValue any, k ...string) map[string]any {
	return mapToA(m, nil, k...).(map[string]any)
}
