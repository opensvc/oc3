package worker

var (
	// hypervisorContainerMergeMap is the merge map rules from hypervisor +
	// encap instance status:
	// map[<hypervisor status>,<encap status>] -> <merged status>
	// TODO: confirm zero status "" have same meaning as "n/a"
	// TODO: or add "undef" status ?
	hypervisorContainerMergeMap = map[string]string{
		"up,up":         "up",
		"up,down":       "warn",
		"up,warn":       "warn",
		"up,n/a":        "up",
		"up,":           "up",
		"up,stdby up":   "up",
		"up,stdby down": "warn",

		"down,up":         "warn",
		"down,down":       "down",
		"down,warn":       "warn",
		"down,n/a":        "down",
		"down,":           "down",
		"down,stdby up":   "stdby up",
		"down,stdby down": "stdby down",

		"warn,up":         "warn",
		"warn,down":       "down",
		"warn,warn":       "warn",
		"warn,n/a":        "warn",
		"warn,":           "warn",
		"warn,stdby up":   "warn",
		"warn,stdby down": "warn",

		"n/a,up":         "up",
		"n/a,down":       "down",
		"n/a,warn":       "warn",
		"n/a,n/a":        "n/a",
		"n/a,":           "n/a",
		"n/a,stdby up":   "stdby up",
		"n/a,stdby down": "stdby down",

		"stdby up,up":         "up",
		"stdby up,down":       "stdby up",
		"stdby up,warn":       "warn",
		"stdby up,n/a":        "stdby up",
		"stdby up,":           "stdby up",
		"stdby up,stdby up":   "stdby up",
		"stdby up,stdby down": "warn",

		"stdby down,up":         "warn",
		"stdby down,down":       "stdby down",
		"stdby down,warn":       "warn",
		"stdby down,n/a":        "stdby down",
		"stdby down,":           "stdby down",
		"stdby down,stdby up":   "warn",
		"stdby down,stdby down": "stdby down",

		",up":         "up",
		",down":       "down",
		",warn":       "warn",
		",n/a":        "n/a",
		",":           "n/a",
		",stdby up":   "stdby up",
		",stdby down": "stdby down",
	}
)
