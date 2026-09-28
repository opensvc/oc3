package worker

import "strings"

// statusGroups is the list of resource driver groups that have a status group
// value (from opensvc v2 DEFAULT_STATUS_GROUPS).
var statusGroups = []string{"ip", "volume", "disk", "fs", "share", "container", "app", "sync", "task"}

// resourcesStatusGroups returns the status of each status group computed from
// the resources status, with the opensvc v2 status_group rules:
//
//   - a group without resources is "n/a"
//   - disabled resources are ignored
//   - resources with the "nostatus" tag are "n/a"
//   - sync resources "up" is "n/a", and "down" is "warn"
//   - optional resources are merged into their group
//
// Encap resources are not ignored: the v3 hypervisor instance status does not
// report the encap resources, and all the resources of an encap instance status
// are encap resources.
func resourcesStatusGroups(resources map[string]any) map[string]string {
	groups := make(map[string]status, len(statusGroups))
	for _, group := range statusGroups {
		groups[group] = statusNotApplicable
	}
	for _, i := range resources {
		resource, ok := i.(map[string]any)
		if !ok {
			continue
		}
		if disabled, _ := resource["disable"].(bool); disabled {
			continue
		}
		resourceType, _ := resource["type"].(string)
		group := strings.SplitN(resourceType, ".", 2)[0]
		groupStatus, ok := groups[group]
		if !ok {
			continue
		}
		rStatus := statusNotApplicable
		if !hasTag(resource, "nostatus") {
			s, _ := resource["status"].(string)
			rStatus = parseStatus(s)
		}
		if group == "sync" {
			switch rStatus {
			case statusUp:
				rStatus = statusNotApplicable
			case statusDown:
				rStatus = statusWarn
			}
		}
		groupStatus.Add(rStatus)
		groups[group] = groupStatus
	}
	result := make(map[string]string, len(groups))
	for group, s := range groups {
		result[group] = s.String()
	}
	return result
}

func hasTag(resource map[string]any, tag string) bool {
	tags, _ := resource["tags"].([]any)
	for _, t := range tags {
		if s, ok := t.(string); ok && s == tag {
			return true
		}
	}
	return false
}
