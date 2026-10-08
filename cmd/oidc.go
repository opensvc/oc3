package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/xauth"
)

func setDefaultOIDCConfig(section string) {
	s := section + ".oidc"
	viper.SetDefault(s+".enable", false)
	viper.SetDefault(s+".scopes", []string{"openid", "email", "profile"})
	viper.SetDefault(s+".link_by_verified_email", false)
	viper.SetDefault(s+".auto_create_users", false)
	viper.SetDefault(s+".session.idle_timeout", "1h")
	viper.SetDefault(s+".session.max_lifetime", "12h")
	viper.SetDefault(s+".session.cookie_secure", true)
	// Users may sign in with their collector password until the operator turns it
	// off, once OpenID Connect is in service.
	viper.SetDefault(section+".auth.basic_users", true)
}

// newOIDC returns the OpenID Connect sign-in configured under <section>.oidc, nil
// when it is not enabled. The client secret is read from client_secret_file, or
// from OC3_SERVER_OIDC_CLIENT_SECRET; never write it in the configuration file.
func newOIDC(ctx context.Context, section string, rdb *redis.Client, db *sql.DB) (*xauth.OIDC, error) {
	s := section + ".oidc"
	if !viper.GetBool(s + ".enable") {
		return nil, nil
	}
	secret := viper.GetString(s + ".client_secret")
	if path := viper.GetString(s + ".client_secret_file"); path != "" {
		var err error
		if secret, err = xauth.ReadSecretFile(path); err != nil {
			return nil, fmt.Errorf("oidc: cannot read client_secret_file: %w", err)
		}
	}
	cfg := xauth.OIDCConfig{
		Enable:                true,
		Issuer:                viper.GetString(s + ".issuer"),
		ClientID:              viper.GetString(s + ".client_id"),
		ClientSecret:          secret,
		RedirectURL:           viper.GetString(s + ".redirect_url"),
		PostLogoutRedirectURL: viper.GetString(s + ".post_logout_redirect_url"),
		Scopes:                viper.GetStringSlice(s + ".scopes"),
		DisplayName:           viper.GetString(s + ".display_name"),
		APIAudience:           viper.GetString(s + ".api_audience"),
		LinkByVerifiedEmail:   viper.GetBool(s + ".link_by_verified_email"),
		AutoCreateUsers:       viper.GetBool(s + ".auto_create_users"),
		IdleTimeout:           viper.GetDuration(s + ".session.idle_timeout"),
		MaxLifetime:           viper.GetDuration(s + ".session.max_lifetime"),
		CookieSecure:          viper.GetBool(s + ".session.cookie_secure"),
	}
	o, err := xauth.NewOIDC(ctx, cfg, rdb, db)
	if err != nil {
		return nil, err
	}
	slog.Info("oidc: enabled", "issuer", cfg.Issuer, "client_id", cfg.ClientID, "redirect_url", cfg.RedirectURL,
		"basic_users", viper.GetBool(section+".auth.basic_users"),
		"auto_create_users", cfg.AutoCreateUsers)
	return o, nil
}
