package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v4"
)

type (
	// provisioningRightsStore is the database access of the provisioning
	// rights.
	provisioningRightsStore interface {
		AppsForGroups(ctx context.Context, groups []string) ([]string, error)
		ClusterNodeApps(ctx context.Context, clusterID string) ([]string, error)
	}

	// provisioningScope tells the clusters a user may provision: those whose
	// apps declared and whose nodes are of apps the user is responsible for,
	// every cluster for a Manager.
	provisioningScope struct {
		store       provisioningRightsStore
		all         bool
		responsible []string
	}
)

// newProvisioningScope returns the provisioning scope of the user.
func newProvisioningScope(c echo.Context, ctx context.Context, store provisioningRightsStore) (*provisioningScope, error) {
	s := &provisioningScope{store: store, all: IsManager(c)}
	if s.all {
		return s, nil
	}
	l, err := store.AppsForGroups(ctx, UserGroupsFromContext(c))
	if err != nil {
		return nil, err
	}
	s.responsible = l
	return s, nil
}

func (s *provisioningScope) isResponsible(app string) bool {
	return slices.ContainsFunc(s.responsible, func(r string) bool { return strings.EqualFold(r, app) })
}

// allows returns "" when the user may provision the cluster clusterID with
// nodes of the apps, else why not. clusterID empty checks the apps only, an
// empty app is ignored.
func (s *provisioningScope) allows(ctx context.Context, clusterID string, apps ...string) (string, error) {
	if s.all {
		return "", nil
	}
	for _, app := range apps {
		if app != "" && !s.isResponsible(app) {
			return "you are not responsible for the app " + app, nil
		}
	}
	if clusterID == "" {
		return "", nil
	}
	nodeApps, err := s.store.ClusterNodeApps(ctx, clusterID)
	if err != nil {
		return "", err
	}
	for _, app := range nodeApps {
		if !s.isResponsible(app) {
			return "you are not responsible for every node of the cluster " + clusterID, nil
		}
	}
	return "", nil
}

// requireProvisioningApps checks the user, NodeManager, may provision the
// cluster clusterID with nodes of the apps: responsible for the apps, and
// for every node the collector has in the cluster, whose members enroll the
// nodes bootstrapped into it. A Manager is responsible for all. clusterID
// empty checks the apps only, an empty app is ignored.
func requireProvisioningApps(c echo.Context, ctx context.Context, log *slog.Logger, store provisioningRightsStore, clusterID string, apps ...string) error {
	if err := requireProvisioning(c); err != nil {
		return err
	}
	scope, err := newProvisioningScope(c, ctx, store)
	if err != nil {
		log.Error("cannot list the apps of the user", "error", err)
		return denyRequest(c, http.StatusInternalServerError, "cannot check the responsibility")
	}
	why, err := scope.allows(ctx, clusterID, apps...)
	if err != nil {
		log.Error("cannot list the apps of the cluster nodes", "error", err)
		return denyRequest(c, http.StatusInternalServerError, "cannot check the responsibility")
	}
	if why != "" {
		return denyRequest(c, http.StatusForbidden, "%s", why)
	}
	return nil
}

// requireProvisioningVisible checks the user may see the declaration of the
// cluster clusterID of the app: one they could provision. Another one is not
// found, as if it did not exist.
func requireProvisioningVisible(c echo.Context, ctx context.Context, log *slog.Logger, store provisioningRightsStore, clusterID, app string) error {
	scope, err := newProvisioningScope(c, ctx, store)
	if err != nil {
		log.Error("cannot list the apps of the user", "error", err)
		return denyRequest(c, http.StatusInternalServerError, "cannot check the responsibility")
	}
	why, err := scope.allows(ctx, clusterID, app)
	if err != nil {
		log.Error("cannot list the apps of the cluster nodes", "error", err)
		return denyRequest(c, http.StatusInternalServerError, "cannot check the responsibility")
	}
	if why != "" {
		return denyRequest(c, http.StatusNotFound, "the cluster %s is not declared", clusterID)
	}
	return nil
}
