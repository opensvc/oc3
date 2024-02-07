package main

import (
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
)

var (
	Redis *redis.Client

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

func initRedis() error {
	address := viper.GetString("Redis.Address")
	password := viper.GetString("Redis.Password")
	database := viper.GetInt("Redis.Database")
	Redis = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       database,
	})
	return nil
}
