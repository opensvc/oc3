package cache

var (
	clientKey = "redisClient"

	KeyGeneric = "oc3:q:generic"

	KeyDaemonPing              = "oc3:q:daemon_ping"
	KeyDaemonPingPending       = "oc3:h:daemon_ping_pending"
	KeyDaemonStatusHash        = "oc3:h:daemon_status"
	KeyDaemonStatusChangesHash = "oc3:h:daemon_status_changes"
	KeyDaemonStatus            = "oc3:q:daemon_status"
	KeyDaemonStatusPending     = "oc3:h:daemon_status_pending"
	KeyDaemonSystem            = "oc3:q:daemon_system"
	KeyDaemonSystemHash        = "oc3:h:daemon_system"
	KeyDaemonSystemPending     = "oc3:h:daemon_system_pending"
	KeyPackagesHash            = "oc3:h:packages"
	KeyPackages                = "oc3:q:packages"
	KeyPackagesPending         = "oc3:h:packages_pending"

	KeyPatchesHash            = "osvc:h:patches"
	KeyPatches                = "osvc:q:patches"
	KeyResinfoHash            = "osvc:h:resinfo"
	KeyResinfo                = "osvc:q:resinfo"
	KeySvcmonUpdate           = "osvc:q:svcmon_update"
	KeySysreport              = "osvc:q:sysreport"
	KeySvcconfHash            = "osvc:h:svcconf"
	KeySvcconf                = "osvc:q:svcconf"
	KeyChecksHash             = "osvc:h:checks"
	KeyChecks                 = "osvc:q:checks"
	KeyUpdateDashNetdevErrors = "osvc:q:update_dash_netdev_errors"
	KeySvcmon                 = "osvc:q:svcmon"
	KeySvcactions             = "osvc:q:svcactions"
	KeyStorage                = "osvc:q:storage"
)
