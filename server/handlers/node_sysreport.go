package serverhandlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/git"
	"github.com/opensvc/oc3/util/logkey"
)

func sysreportRepos() git.Sysreport {
	return git.Sysreport{Dir: viper.GetString("server.directories.sysreport")}
}

// sysreportDatePattern bounds the dates passed to git --since and --until.
var sysreportDatePattern = regexp.MustCompile(`^[0-9][0-9T:+. Z-]*$`)

// sysreportAccess tells what the caller may read of a node's sysreport, as the
// historical collector's lib_sysreport did: the paths matching a secure pattern
// are sensitive, and their content is shown only to managers, to the members of
// the node's responsible team, and to the groups an authorization names for
// the path and a filterset holding the node. Without any secure pattern, or
// with one that does not compile, every path is sensitive.
type sysreportAccess struct {
	secure *regexp.Regexp // nil: every path is sensitive
	full   bool
	allows []*regexp.Regexp
}

// anchored compiles a pattern as python's re.match() applies it: from the start
// of the path.
func anchored(pattern string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + pattern + ")")
}

// check returns whether the path is sensitive, and whether its content is
// withheld from the caller. The path is the one of the repository.
func (acc sysreportAccess) check(path string) (secure, restricted bool) {
	secure = acc.secure == nil || acc.secure.MatchString(path)
	if !secure || acc.full {
		return secure, false
	}
	for _, allow := range acc.allows {
		if allow.MatchString(path) {
			return true, false
		}
	}
	return true, true
}

// nodeSysreportAccess checks that the caller may see the node and returns what
// they may read of its sysreport. A nil access comes with the response already
// written.
func (a *Api) nodeSysreportAccess(c echo.Context, ctx context.Context, handler, nodeID string) (*sysreportAccess, error) {
	log := echolog.GetLogHandler(c, handler)
	odb := a.ODB
	groups := UserGroupsFromContext(c)

	team, found, err := odb.NodeTeamResponsible(ctx, nodeID)
	if err != nil {
		log.Error("cannot read node", logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot read node")
	}
	if !found {
		return nil, JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeID)
	}
	visible, err := odb.NodeResponsible(ctx, nodeID, groups, IsManager(c))
	if err != nil {
		log.Error("cannot check node access", logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot check node access")
	}
	if !visible {
		return nil, JSONProblemf(c, http.StatusForbidden, "not responsible for node %s", nodeID)
	}

	acc := &sysreportAccess{full: IsManager(c) || (team != "" && slices.Contains(groups, team))}
	patterns, err := odb.SysrepSecurePatterns(ctx)
	if err != nil {
		log.Error("cannot read secure patterns", logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot read secure patterns")
	}
	if len(patterns) > 0 {
		if re, err := anchored(strings.Join(patterns, "|")); err == nil {
			acc.secure = re
		} else {
			log.Warn("invalid secure pattern: every sysreport path is treated as sensitive", logkey.Error, err)
		}
	}
	if acc.full {
		return acc, nil
	}

	userID := authUserID(c)
	if userID == nil {
		return acc, nil
	}
	groupIDs, err := odb.UserGroupIDs(ctx, *userID)
	if err != nil {
		log.Error("cannot list user groups", logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot list user groups")
	}
	allows, err := odb.SysrepAllows(ctx, groupIDs)
	if err != nil {
		log.Error("cannot read authorizations", logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot read authorizations")
	}
	inFilterset := map[int]bool{}
	for _, allow := range allows {
		in, known := inFilterset[allow.FsetID]
		if !known {
			nodeIDs, err := odb.ResolveFilterset(ctx, allow.FsetID, "node_id")
			if err != nil {
				// An authorization that cannot be resolved grants nothing.
				log.Warn("cannot resolve the filterset of an authorization", "fset_id", allow.FsetID, logkey.Error, err)
			}
			in = err == nil && slices.Contains(nodeIDs, nodeID)
			inFilterset[allow.FsetID] = in
		}
		if !in {
			continue
		}
		if re, err := anchored(allow.Pattern); err == nil {
			acc.allows = append(acc.allows, re)
		}
	}
	return acc, nil
}

func sysreportFile(path string) map[string]any {
	display, kind := git.SysreportDisplay(path)
	return map[string]any{"path": display, "kind": kind}
}

// GetNodeSysreport handles GET /nodes/{node_id}/sysreport: the changes of the
// files and command outputs the node reports, newest first.
func (a *Api) GetNodeSysreport(c echo.Context, nodeId string, params server.GetNodeSysreportParams) error {
	ctx := c.Request().Context()
	if acc, err := a.nodeSysreportAccess(c, ctx, "GetNodeSysreport", nodeId); acc == nil {
		return err
	}
	var since, until string
	for _, bound := range []struct {
		name  string
		value *string
		into  *string
	}{{"begin", params.Begin, &since}, {"end", params.End, &until}} {
		if bound.value == nil || *bound.value == "" {
			continue
		}
		if !sysreportDatePattern.MatchString(*bound.value) {
			return JSONProblemf(c, http.StatusBadRequest, "%s must be a date, got %q", bound.name, *bound.value)
		}
		*bound.into = *bound.value
	}
	limit, offset := 50, 0
	if params.Limit != nil {
		limit = *params.Limit
	}
	if params.Offset != nil && *params.Offset > 0 {
		offset = *params.Offset
	}
	needle := ""
	if params.Path != nil {
		needle = strings.ToLower(strings.TrimSpace(*params.Path))
	}

	changes, err := sysreportRepos().Timeline(nodeId, since, until)
	if err != nil && !errors.Is(err, git.ErrNoSysreport) {
		echolog.GetLogHandler(c, "GetNodeSysreport").Error("cannot read sysreport", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the sysreport")
	}

	data := []map[string]any{}
	total := 0
	for _, change := range changes {
		files := []map[string]any{}
		for _, stat := range change.Files {
			file := sysreportFile(stat.Path)
			if needle != "" && !strings.Contains(strings.ToLower(file["path"].(string)), needle) {
				continue
			}
			file["added"] = stat.Added
			file["deleted"] = stat.Deleted
			file["binary"] = stat.Binary
			files = append(files, file)
		}
		// A change with no file left, all filtered out or purged from the history,
		// is not listed.
		if len(files) == 0 {
			continue
		}
		total++
		if total <= offset || (limit > 0 && len(data) >= limit) {
			continue
		}
		data = append(data, map[string]any{"cid": change.ID, "date": change.Date, "initial": change.Initial, "files": files})
	}
	return c.JSON(http.StatusOK, map[string]any{
		"data": data,
		"meta": map[string]any{"total": total, "limit": limit, "offset": offset, "count": len(data)},
	})
}

// GetNodeSysreportChange handles GET /nodes/{node_id}/sysreport/{cid}: the
// change a revision made to each file, as a unified diff, withheld for the
// sensitive paths the caller may not read.
func (a *Api) GetNodeSysreportChange(c echo.Context, nodeId string, cid string) error {
	ctx := c.Request().Context()
	acc, err := a.nodeSysreportAccess(c, ctx, "GetNodeSysreportChange", nodeId)
	if acc == nil {
		return err
	}
	date, diffs, err := sysreportRepos().Show(nodeId, cid)
	if err != nil {
		return sysreportProblem(c, "GetNodeSysreportChange", err, cid)
	}
	return c.JSON(http.StatusOK, map[string]any{"data": map[string]any{"cid": cid, "date": date, "files": sysreportDiffFiles(acc, diffs)}})
}

// sysreportDiffFiles presents the diffs of a change, the content of the
// sensitive paths the caller may not read withheld.
func sysreportDiffFiles(acc *sysreportAccess, diffs []git.SysreportFileDiff) []map[string]any {
	files := []map[string]any{}
	for _, d := range diffs {
		file := sysreportFile(d.Path)
		secure, restricted := acc.check(d.Path)
		file["added"] = d.Added
		file["deleted"] = d.Deleted
		file["binary"] = d.Binary
		file["secure"] = secure
		file["restricted"] = restricted
		file["truncated"] = d.Truncated && !restricted
		if !restricted {
			file["diff"] = strings.ToValidUTF8(d.Diff, "\uFFFD")
		}
		files = append(files, file)
	}
	return files
}

// GetNodeSysreportTimediff handles GET /nodes/{node_id}/sysreport/timediff: what
// changed in each file between two states of the node's sysreport, each given
// as a date — the last report made at or before it — or as a revision. The end
// defaults to the latest report. Sensitive paths are withheld as for a change.
func (a *Api) GetNodeSysreportTimediff(c echo.Context, nodeId string, params server.GetNodeSysreportTimediffParams) error {
	ctx := c.Request().Context()
	acc, err := a.nodeSysreportAccess(c, ctx, "GetNodeSysreportTimediff", nodeId)
	if acc == nil {
		return err
	}
	repos := sysreportRepos()
	end := ""
	if params.End != nil {
		end = strings.TrimSpace(*params.End)
	}
	points := make([]git.SysreportPoint, 2)
	for i, spec := range []string{strings.TrimSpace(params.Begin), end} {
		// A date holds the separators of a date or a time; a revision is a commit
		// id or a ref.
		isDate := sysreportDatePattern.MatchString(spec) && strings.ContainsAny(spec, "-:")
		if spec != "" && !isDate && !git.ValidRev(spec) {
			return JSONProblemf(c, http.StatusBadRequest, "begin and end must be a date or a revision, got %q", spec)
		}
		point, err := repos.At(nodeId, spec, isDate)
		if err != nil {
			return sysreportProblem(c, "GetNodeSysreportTimediff", err, spec)
		}
		points[i] = point
	}
	diffs, err := repos.Diff(nodeId, points[0], points[1])
	if err != nil {
		return sysreportProblem(c, "GetNodeSysreportTimediff", err, "")
	}
	point := func(p git.SysreportPoint) map[string]any {
		return map[string]any{"cid": p.ID, "date": p.Date}
	}
	return c.JSON(http.StatusOK, map[string]any{"data": map[string]any{
		"begin": point(points[0]), "end": point(points[1]), "files": sysreportDiffFiles(acc, diffs),
	}})
}

// GetNodeSysreportTree handles GET /nodes/{node_id}/sysreport/{cid}/tree: the
// files and command outputs of the node's sysreport at a revision.
func (a *Api) GetNodeSysreportTree(c echo.Context, nodeId string, cid string) error {
	ctx := c.Request().Context()
	acc, err := a.nodeSysreportAccess(c, ctx, "GetNodeSysreportTree", nodeId)
	if acc == nil {
		return err
	}
	entries, err := sysreportRepos().Tree(nodeId, cid)
	if err != nil {
		return sysreportProblem(c, "GetNodeSysreportTree", err, cid)
	}
	data := []map[string]any{}
	for _, entry := range entries {
		file := sysreportFile(entry.Path)
		secure, restricted := acc.check(entry.Path)
		file["oid"] = entry.OID
		file["size"] = entry.Size
		file["secure"] = secure
		file["restricted"] = restricted
		data = append(data, file)
	}
	return c.JSON(http.StatusOK, map[string]any{"data": data})
}

// GetNodeSysreportFile handles GET /nodes/{node_id}/sysreport/{cid}/tree/{oid}:
// the content of a file of the node's sysreport at a revision. The object must
// belong to that revision, and a sensitive path the caller may not read is
// refused.
func (a *Api) GetNodeSysreportFile(c echo.Context, nodeId string, cid string, oid string) error {
	ctx := c.Request().Context()
	acc, err := a.nodeSysreportAccess(c, ctx, "GetNodeSysreportFile", nodeId)
	if acc == nil {
		return err
	}
	repos := sysreportRepos()
	entries, err := repos.Tree(nodeId, cid)
	if err != nil {
		return sysreportProblem(c, "GetNodeSysreportFile", err, cid)
	}
	at := slices.IndexFunc(entries, func(e git.SysreportEntry) bool { return e.OID == oid })
	if at < 0 {
		return JSONProblemf(c, http.StatusNotFound, "no file %s in revision %s", oid, cid)
	}
	entry := entries[at]
	secure, restricted := acc.check(entry.Path)
	if restricted {
		return JSONProblemf(c, http.StatusForbidden, "not allowed to read this file")
	}
	content, truncated, err := repos.Blob(nodeId, oid)
	if err != nil {
		return sysreportProblem(c, "GetNodeSysreportFile", err, cid)
	}
	file := sysreportFile(entry.Path)
	file["oid"] = oid
	file["size"] = entry.Size
	file["secure"] = secure
	file["truncated"] = truncated
	// Not text: a NUL byte, or bytes that are not UTF-8 beyond a cut rune.
	binary := bytes.IndexByte(content, 0) >= 0 || (!utf8.Valid(content) && !truncated)
	file["binary"] = binary
	if !binary {
		file["content"] = strings.ToValidUTF8(string(content), "\uFFFD")
	}
	return c.JSON(http.StatusOK, map[string]any{"data": file})
}

func sysreportProblem(c echo.Context, handler string, err error, cid string) error {
	switch {
	case errors.Is(err, git.ErrNoSysreport):
		return JSONProblemf(c, http.StatusNotFound, "this node has no sysreport")
	case errors.Is(err, git.ErrNoRevision):
		return JSONProblemf(c, http.StatusNotFound, "no revision %s in the sysreport of this node", cid)
	}
	echolog.GetLogHandler(c, handler).Error("cannot read sysreport", logkey.Error, err)
	return JSONProblemf(c, http.StatusInternalServerError, "cannot read the sysreport")
}
