package cmd

import (
	"fmt"
	"log/slog"
	"net/url"

	"github.com/opensvc/oc3/messenger"
	"github.com/spf13/viper"
)

func startMessenger() error {
	if err := setup(); err != nil {
		return err
	}

	u, err := url.Parse(viper.GetString("messenger.url"))
	if err != nil {
		slog.Warn(fmt.Sprintf("parsing messenger.url: %v", err))
		return err
	}

	cometCmd := messenger.CmdComet{
		Address:      u.Hostname(),
		Port:         u.Port(),
		Key:          viper.GetString("messenger.key"),
		RequireToken: viper.GetBool("messenger.require_token"),
		CertFile:     viper.GetString("messenger.cert_file"),
		KeyFile:      viper.GetString("messenger.key_file"),
	}

	return cometCmd.Run()
}
