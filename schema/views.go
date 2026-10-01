package schema

// Views. schema/gen skips them (tables.go only lists tables), so the ones the
// server reads through the query builder are declared here by hand.
var (
	TVSwitches     = &Table{Name: "v_switches"}
	TVObsolescence = &Table{Name: "v_obsolescence"}
)

// Columns of v_switches: the switches table, plus the node and the name of what
// is plugged on the other end of each port (a node, an array or another switch).
var (
	VSwitchesID          = &Col{T: TVSwitches, Name: "id", Nullable: false}
	VSwitchesSwName      = &Col{T: TVSwitches, Name: "sw_name", Nullable: false}
	VSwitchesSwSlot      = &Col{T: TVSwitches, Name: "sw_slot", Nullable: true}
	VSwitchesSwPort      = &Col{T: TVSwitches, Name: "sw_port", Nullable: true}
	VSwitchesSwPortspeed = &Col{T: TVSwitches, Name: "sw_portspeed", Nullable: true}
	VSwitchesSwPortnego  = &Col{T: TVSwitches, Name: "sw_portnego", Nullable: true}
	VSwitchesSwPorttype  = &Col{T: TVSwitches, Name: "sw_porttype", Nullable: true}
	VSwitchesSwPortstate = &Col{T: TVSwitches, Name: "sw_portstate", Nullable: true}
	VSwitchesSwPortname  = &Col{T: TVSwitches, Name: "sw_portname", Nullable: true}
	VSwitchesSwRportname = &Col{T: TVSwitches, Name: "sw_rportname", Nullable: true}
	VSwitchesSwUpdated   = &Col{T: TVSwitches, Name: "sw_updated", Nullable: false}
	VSwitchesSwFabric    = &Col{T: TVSwitches, Name: "sw_fabric", Nullable: true}
	VSwitchesSwIndex     = &Col{T: TVSwitches, Name: "sw_index", Nullable: true}
	VSwitchesNodeID      = &Col{T: TVSwitches, Name: "node_id", Nullable: true}
	VSwitchesSwRname     = &Col{T: TVSwitches, Name: "sw_rname", Nullable: true}
)

// Columns of v_obsolescence: the obsolescence settings, with the number of nodes
// each model or release concerns.
var (
	VObsolescenceID                   = &Col{T: TVObsolescence, Name: "id", Nullable: false}
	VObsolescenceObsType              = &Col{T: TVObsolescence, Name: "obs_type", Nullable: false}
	VObsolescenceObsName              = &Col{T: TVObsolescence, Name: "obs_name", Nullable: false}
	VObsolescenceObsWarnDate          = &Col{T: TVObsolescence, Name: "obs_warn_date", Nullable: true}
	VObsolescenceObsAlertDate         = &Col{T: TVObsolescence, Name: "obs_alert_date", Nullable: true}
	VObsolescenceObsWarnDateUpdatedBy = &Col{T: TVObsolescence, Name: "obs_warn_date_updated_by", Nullable: false}
	VObsolescenceObsAlertDateUpdBy    = &Col{T: TVObsolescence, Name: "obs_alert_date_updated_by", Nullable: false}
	VObsolescenceObsWarnDateUpdated   = &Col{T: TVObsolescence, Name: "obs_warn_date_updated", Nullable: false}
	VObsolescenceObsAlertDateUpdated  = &Col{T: TVObsolescence, Name: "obs_alert_date_updated", Nullable: false}
	VObsolescenceObsCount             = &Col{T: TVObsolescence, Name: "obs_count", Nullable: false}
)
