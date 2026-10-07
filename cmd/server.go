package cmd

import (
	"context"
	"database/sql"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	api "github.com/opensvc/oc3/server"
	handlers "github.com/opensvc/oc3/server/handlers"
	"github.com/opensvc/oc3/xauth"
)

type (
	server struct {
		db      *sql.DB
		section string
		redis   *redis.Client
		// oidc is the OpenID Connect sign-in; nil when server.oidc.enable is false.
		oidc *xauth.OIDC
	}
)

func newServer() (*server, error) {
	db, err := newDatabase()
	if err != nil {
		return nil, err
	}
	t := &server{db: db, section: sectionServer, redis: newRedis()}
	// A wrong OIDC configuration stops the server rather than leaving it to run
	// with weaker settings than the operator asked for.
	if t.oidc, err = newOIDC(context.Background(), t.section, t.redis, db); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *server) Section() string { return t.section }

func (t *server) apiRegister(e *echo.Echo) {
	odb := cdb.New(t.db)
	handler := &handlers.Api{
		DB:          t.db,
		ODB:         odb,
		Redis:       t.redis,
		UI:          viper.GetBool(t.section + ".ui.enable"),
		SyncTimeout: viper.GetDuration(t.section + ".sync.timeout"),
		SubSystem:   t.section,
		OIDC:        t.oidc,
		BasicUsers:  viper.GetBool(t.section + ".auth.basic_users"),
	}
	// With a messenger, the changes made through the api are announced like those
	// of the workers, and the websocket clients get their tokens from the api.
	if viper.GetString("messenger.url") != "" {
		ev := newEv()
		odb.CreateSession(ev)
		handler.Ev = ev
		handler.Realtime = ev
	} else {
		odb.CreateSession(nil)
	}
	api.RegisterHandlersWithBaseURL(e, handler, pathApi)
}

func (t *server) docMiddleware() echo.MiddlewareFunc {
	return handlers.UIMiddleware(context.Background(), pathApi, pathSpec)
}

// authMiddleware authenticates the request, then lets a member of the
// Manager make it as another user.
func (t *server) authMiddleware(publicPath, publicPrefix []string) echo.MiddlewareFunc {
	authenticate := t.authenticateMiddleware(publicPath, publicPrefix)
	// Right after authentication: a request riding on the session cookie must come
	// from the SPA before anything acts on it, impersonation included.
	csrf := handlers.CSRFMiddleware(t.oidc)
	impersonate := handlers.ImpersonateMiddleware(t.db)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return authenticate(csrf(impersonate(next)))
	}
}

func (t *server) authenticateMiddleware(publicPath, publicPrefix []string) echo.MiddlewareFunc {
	// The endpoints of the OpenID Connect sign-in lead to the authentication: they
	// are public, each checking what it needs. Listed one by one, so that the
	// other /auth endpoints keep their own strategies; login and callback by
	// prefix, as their query string is part of the request URI.
	publicPath = append(publicPath,
		pathApi+"/auth/info", pathApi+"/auth/logout", pathApi+"/auth/backchannel-logout")
	publicPrefix = append(publicPrefix, pathApi+"/auth/login", pathApi+"/auth/callback")

	strategies := []auth.Strategy{
		xauth.NewPublicStrategy(publicPath, publicPrefix),
		xauth.NewAnonRegister(),
	}
	if t.oidc != nil {
		strategies = append(strategies, xauth.NewOIDCSession(t.oidc, t.db), xauth.NewOIDCBearer(t.oidc, t.db))
	}
	if viper.GetBool(t.section + ".auth.basic_users") {
		strategies = append(strategies, xauth.NewBasicWeb2py(t.db, viper.GetString("w2p_hmac")))
	}
	strategies = append(strategies, xauth.NewBasicNode(t.db))
	return handlers.AuthMiddleware(union.New(strategies...))
}
