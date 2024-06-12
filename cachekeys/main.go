package cachekeys

var (
	FeedDaemonPingQ        = "oc3:q:feed_daemon_ping"
	FeedDaemonPingPendingH = "oc3:h:feed_daemon_ping_pending"

	FeedDaemonStatusChangesH = "oc3:h:feed_daemon_status_changes"
	FeedDaemonStatusH        = "oc3:h:feed_daemon_status"
	FeedDaemonStatusQ        = "oc3:q:feed_daemon_status"
	FeedDaemonStatusPendingH = "oc3:h:feed_daemon_status_pending"

	FeedObjectConfigH        = "oc3:h:feed_object_config"
	FeedObjectConfigQ        = "oc3:q:feed_object_config"
	FeedObjectConfigPendingH = "oc3:h:feed_object_config_pending"

	FeedSystemQ        = "oc3:q:feed_system"
	FeedSystemH        = "oc3:h:feed_system"
	FeedSystemPendingH = "oc3:h:feed_system_pending"
)
