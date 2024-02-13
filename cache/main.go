package cache

var (
	clientKey = "redisClient"

	KeySystem     = "oc3:q:system"
	KeySystemHash = "oc3:h:system"
	KeyGeneric    = "oc3:q:generic"

	KeyDaemonStatusHash        = "oc3:h:daemon_status"
	KeyDaemonStatusChangesHash = "oc3:h:daemon_status_changes"
	KeyDaemonStatus            = "oc3:q:daemon_status"
	KeyDaemonStatusPending     = "oc3:h:daemon_status_pending"

	KeydaemonPing             = "osvc:q:daemon_ping"
	KeyPackagesHash           = "osvc:h:packages"
	KeyPackages               = "osvc:q:packages"
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
