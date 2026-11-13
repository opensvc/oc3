package cmd

import (
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/oidcprovider"
)

// pathOIDCProvider is where the endpoints of the collector as the OpenID
// Connect provider of the om3 clusters are served: the issuer URL is the public
// URL of the API followed by /oidc.
const pathOIDCProvider = pathApi + "/oidc"

func setDefaultOIDCProviderConfig(section string) {
	s := section + ".oidc_provider"
	viper.SetDefault(s+".enable", false)
	viper.SetDefault(s+".access_token_ttl", "10m")
	viper.SetDefault(s+".refresh_token.idle_timeout", "1h")
	viper.SetDefault(s+".refresh_token.max_lifetime", "12h")
	viper.SetDefault(s+".redirect_hosts", []string{})
}

// newOIDCProvider returns the collector as the OpenID Connect provider of the
// om3 clusters, as configured under <section>.oidc_provider, nil when it is not
// enabled:
//
//	issuer: the public URL of the API followed by /oidc, as the nodes and the
//	  browsers reach it (https://collector.example.com/api/oidc)
//	ui_url: the URL of the collector SPA, where the users sign in; by default
//	  the scheme and host of the issuer
//	signing_key_files: PEM private keys (RSA 2048+, ECDSA P-256 or P-384), the
//	  first one signing, all published: a new key is added first, the old one
//	  kept second until the tokens it signed expired
//	redirect_hosts: host name patterns (*.example.com) allowed in the
//	  redirection URIs of every cluster, beside its nodes
//
// A configuration enabling it with a missing or unreadable key stops the server.
func newOIDCProvider(section string, rdb *redis.Client) (*oidcprovider.Provider, error) {
	s := section + ".oidc_provider"
	if !viper.GetBool(s + ".enable") {
		return nil, nil
	}
	keys, err := oidcprovider.LoadKeys(viper.GetStringSlice(s + ".signing_key_files"))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", s, err)
	}
	p, err := oidcprovider.New(oidcprovider.Config{
		Issuer:               viper.GetString(s + ".issuer"),
		UIURL:                viper.GetString(s + ".ui_url"),
		AccessTokenTTL:       viper.GetDuration(s + ".access_token_ttl"),
		RefreshIdleTimeout:   viper.GetDuration(s + ".refresh_token.idle_timeout"),
		RefreshMaxLifetime:   viper.GetDuration(s + ".refresh_token.max_lifetime"),
		RedirectHostPatterns: viper.GetStringSlice(s + ".redirect_hosts"),
	}, keys, rdb)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", s, err)
	}
	cfg := p.Config()
	slog.Info("oidc provider: enabled", "issuer", cfg.Issuer, "ui_url", cfg.UIURL,
		"alg", keys.Algorithm(), "keys", len(keys.JWKS().Keys), "redirect_hosts", cfg.RedirectHostPatterns)
	return p, nil
}
