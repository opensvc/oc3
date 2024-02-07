package cache

import (
	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
)

var (
	clientKey = "redisClient"

	KeyDaemonStatusHash        = "osvc:h:daemon_status"
	KeyDaemonStatusChangesHash = "osvc:h:daemon_status_changes"
	KeyDaemonStatus            = "osvc:q:daemon_status"
	KeydaemonPing              = "osvc:q:daemon_ping"
	KeyPackagesHash            = "osvc:h:packages"
	KeyPackages                = "osvc:q:packages"
	KeyPatchesHash             = "osvc:h:patches"
	KeyPatches                 = "osvc:q:patches"
	KeyResinfoHash             = "osvc:h:resinfo"
	KeyResinfo                 = "osvc:q:resinfo"
	KeySvcmonUpdate            = "osvc:q:svcmon_update"
	KeySysreport               = "osvc:q:sysreport"
	KeyAssetHash               = "osvc:h:asset"
	KeyAsset                   = "osvc:q:asset"
	KeySvcconfHash             = "osvc:h:svcconf"
	KeySvcconf                 = "osvc:q:svcconf"
	KeyGeneric                 = "osvc:q:generic"
	KeyChecksHash              = "osvc:h:checks"
	KeyCHECKS                  = "osvc:q:checks"
	KeyUpdateDashNetdevErrors  = "osvc:q:update_dash_netdev_errors"
	KeySvcmon                  = "osvc:q:svcmon"
	KeySvcactions              = "osvc:q:svcactions"
	KeyStorage                 = "osvc:q:storage"
)

func NewClient(address, password string, database int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       database,
	})
}

func ContextWithClient(c echo.Context, client *redis.Client) echo.Context {
	c.Set(clientKey, client)
	return c
}

func ClientFromContext(c echo.Context) *redis.Client {
	return c.Get(clientKey).(*redis.Client)
}

func RedisMiddleware(address, password string, database int) echo.MiddlewareFunc {
	client := NewClient(address, password, database)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c = ContextWithClient(c, client)
			return next(c)
		}
	}
}
