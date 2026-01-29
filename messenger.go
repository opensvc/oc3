package main

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

	u, err := url.Parse(viper.GetString("websocket.url"))
	if err != nil {
		slog.Warn(fmt.Sprintf("parsing websocket.url: %v", err))
		return err
	}

	cometCmd := messenger.CmdComet{
		Address:      u.Hostname(),
		Port:         u.Port(),
		Key:          viper.GetString("websocket.key"),
		RequireToken: viper.GetBool("websocket.require_token"),
		CertFile:     viper.GetString("websocket.cert_file"),
		KeyFile:      viper.GetString("websocket.key_file"),
	}

	return cometCmd.Run()
}
