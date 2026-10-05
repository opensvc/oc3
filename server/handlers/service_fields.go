package serverhandlers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/opensvc/oc3/server"
)

type serviceBodyFields struct {
	Svcname              *string
	SvcApp               *string
	SvcEnv               *string
	SvcComment           *string
	SvcNodes             *string
	SvcDrpnode           *string
	SvcDrpnodes          *string
	SvcAutostart         *string
	SvcDrptype           *string
	SvcDrnoaction        *string
	SvcMetrocluster      *string
	SvcWave              *int
	SvcTopology          *string
	SvcFlexMinNodes      *int
	SvcFlexMaxNodes      *int
	SvcFlexCpuLowThresh  *int
	SvcFlexCpuHighThresh *int
	SvcHa                *string
	SvcFrozen            *string
	SvcProvisioned       *string
	SvcPlacement         *string
	SvcNotifications     *bool
	SvcSnoozeTill        *string
	// SvcSla is the availability target in percent, "" to remove it; checked by
	// parseSLA before reaching the fields.
	SvcSla *string
}

func (f serviceBodyFields) toFields() map[string]any {
	m := map[string]any{}
	setStr := func(key string, v *string) {
		if v != nil {
			m[key] = *v
		}
	}
	setInt := func(key string, v *int) {
		if v != nil {
			m[key] = *v
		}
	}
	setBool := func(key string, v *bool) {
		if v != nil {
			m[key] = *v
		}
	}
	setStr("svcname", f.Svcname)
	setStr("svc_app", f.SvcApp)
	setStr("svc_env", f.SvcEnv)
	setStr("svc_comment", f.SvcComment)
	setStr("svc_nodes", f.SvcNodes)
	setStr("svc_drpnode", f.SvcDrpnode)
	setStr("svc_drpnodes", f.SvcDrpnodes)
	setStr("svc_autostart", f.SvcAutostart)
	setStr("svc_drptype", f.SvcDrptype)
	setStr("svc_drnoaction", f.SvcDrnoaction)
	setStr("svc_metrocluster", f.SvcMetrocluster)
	setInt("svc_wave", f.SvcWave)
	setStr("svc_topology", f.SvcTopology)
	setInt("svc_flex_min_nodes", f.SvcFlexMinNodes)
	setInt("svc_flex_max_nodes", f.SvcFlexMaxNodes)
	setInt("svc_flex_cpu_low_threshold", f.SvcFlexCpuLowThresh)
	setInt("svc_flex_cpu_high_threshold", f.SvcFlexCpuHighThresh)
	setStr("svc_ha", f.SvcHa)
	setStr("svc_frozen", f.SvcFrozen)
	setStr("svc_provisioned", f.SvcProvisioned)
	setStr("svc_placement", f.SvcPlacement)
	setBool("svc_notifications", f.SvcNotifications)
	setStr("svc_snooze_till", f.SvcSnoozeTill)
	if f.SvcSla != nil {
		if sla, err := parseSLA(*f.SvcSla); err == nil {
			m["svc_sla"] = sla
		}
	}
	return m
}

func serviceBodyFieldsFromPostService(b server.PostServiceJSONRequestBody) serviceBodyFields {
	return serviceBodyFields{
		Svcname: b.Svcname, SvcApp: b.SvcApp, SvcEnv: b.SvcEnv, SvcComment: b.SvcComment,
		SvcNodes: b.SvcNodes, SvcDrpnode: b.SvcDrpnode, SvcDrpnodes: b.SvcDrpnodes,
		SvcAutostart: b.SvcAutostart, SvcDrptype: b.SvcDrptype, SvcDrnoaction: b.SvcDrnoaction,
		SvcMetrocluster: b.SvcMetrocluster, SvcWave: b.SvcWave, SvcTopology: b.SvcTopology,
		SvcFlexMinNodes: b.SvcFlexMinNodes, SvcFlexMaxNodes: b.SvcFlexMaxNodes,
		SvcFlexCpuLowThresh: b.SvcFlexCpuLowThreshold, SvcFlexCpuHighThresh: b.SvcFlexCpuHighThreshold,
		SvcHa: b.SvcHa, SvcFrozen: b.SvcFrozen, SvcProvisioned: b.SvcProvisioned,
		SvcPlacement: b.SvcPlacement, SvcNotifications: b.SvcNotifications, SvcSnoozeTill: b.SvcSnoozeTill,
		SvcSla: b.SvcSla,
	}
}

// parseSLA reads an availability target: a percent between 0 and 100, or the
// empty string, which removes the SLA (nil, stored as NULL).
func parseSLA(s string) (any, error) {
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "%"))
	if s == "" {
		return nil, nil
	}
	v, err := strconv.ParseFloat(strings.Replace(s, ",", ".", 1), 64)
	if err != nil || v < 0 || v > 100 {
		return nil, fmt.Errorf("the SLA must be a percent between 0 and 100, or empty: %q", s)
	}
	return v, nil
}

func serviceBodyFieldsFromPostServices(b server.PostServicesJSONRequestBody) serviceBodyFields {
	return serviceBodyFields{
		Svcname: b.Svcname, SvcApp: b.SvcApp, SvcEnv: b.SvcEnv, SvcComment: b.SvcComment,
		SvcNodes: b.SvcNodes, SvcDrpnode: b.SvcDrpnode, SvcDrpnodes: b.SvcDrpnodes,
		SvcAutostart: b.SvcAutostart, SvcDrptype: b.SvcDrptype, SvcDrnoaction: b.SvcDrnoaction,
		SvcMetrocluster: b.SvcMetrocluster, SvcWave: b.SvcWave, SvcTopology: b.SvcTopology,
		SvcFlexMinNodes: b.SvcFlexMinNodes, SvcFlexMaxNodes: b.SvcFlexMaxNodes,
		SvcFlexCpuLowThresh: b.SvcFlexCpuLowThreshold, SvcFlexCpuHighThresh: b.SvcFlexCpuHighThreshold,
		SvcHa: b.SvcHa, SvcFrozen: b.SvcFrozen, SvcProvisioned: b.SvcProvisioned,
		SvcPlacement: b.SvcPlacement, SvcNotifications: b.SvcNotifications, SvcSnoozeTill: b.SvcSnoozeTill,
	}
}
