package worker

var (
	// hypervisorContainerMergeMap is the merge map rules from hypervisor +
	// encap instance status:
	// map[<hypervisor status>,<encap status>] -> <merged status>
	// TODO: confirm zero status "" have same meaning as "n/a"
	// TODO: or add "undef" status ?
	hypervisorContainerMergeMap = map[string]string{
		"up,up":           "up",
		"up,down":         "warn",
		"up,warn":         "warn",
		"up,n/a":          "up",
		"up,":             "up",
		"up,standby up":   "up",
		"up,standby down": "warn",

		"down,up":           "warn",
		"down,down":         "down",
		"down,warn":         "warn",
		"down,n/a":          "down",
		"down,":             "down",
		"down,standby up":   "standby up",
		"down,standby down": "standby down",

		"warn,up":           "warn",
		"warn,down":         "down",
		"warn,warn":         "warn",
		"warn,n/a":          "warn",
		"warn,":             "warn",
		"warn,standby up":   "warn",
		"warn,standby down": "warn",

		"n/a,up":           "up",
		"n/a,down":         "down",
		"n/a,warn":         "warn",
		"n/a,n/a":          "n/a",
		"n/a,":             "n/a",
		"n/a,standby up":   "standby up",
		"n/a,standby down": "standby down",

		"standby up,up":           "up",
		"standby up,down":         "standby up",
		"standby up,warn":         "warn",
		"standby up,n/a":          "standby up",
		"standby up,":             "standby up",
		"standby up,standby up":   "standby up",
		"standby up,standby down": "warn",

		"standby down,up":           "warn",
		"standby down,down":         "standby down",
		"standby down,warn":         "warn",
		"standby down,n/a":          "standby down",
		"standby down,":             "standby down",
		"standby down,standby up":   "warn",
		"standby down,standby down": "standby down",

		",up":           "up",
		",down":         "down",
		",warn":         "warn",
		",n/a":          "n/a",
		",":             "n/a",
		",standby up":   "standby up",
		",standby down": "standby down",
	}
)
