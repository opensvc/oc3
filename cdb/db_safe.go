package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SafeAccess is who asks for safe files: a Manager sees and manages them all; a
// user those they uploaded or that a group of theirs is published (or made
// responsible); a node those published to (or made the responsibility of) its
// responsible team, as lib_safe_check_file_publication() does.
type SafeAccess struct {
	Manager bool
	UserID  *int64
	// Groups are the roles of the user's groups.
	Groups []string
	NodeID string
}

// SafeFile is a row of the safe: a version of a file, with what opens it when it
// is encrypted at rest.
type SafeFile struct {
	ID           int64
	Uploader     *int64
	UploadedFrom string
	UploadedDate string
	Name         string
	Size         int64
	UUID         string
	MD5          string
	SHA256       string
	EncVersion   *int
	EncUUID      string
	EncKeyID     string
	WrappedKey   []byte
}

// Encrypted tells whether the file the row holds is the one its encryption
// columns describe: a version uploaded since by the historical collector, which
// does not know them, is plaintext.
func (f *SafeFile) Encrypted() bool {
	return f.EncVersion != nil && f.EncUUID != "" && f.EncUUID == f.UUID
}

// safeGroupCond is the condition on safe.id of the files a group of the caller is
// linked to in a table, safe_team_publication or safe_team_responsible.
func safeGroupCond(table string, a SafeAccess) (string, []any) {
	switch {
	case a.Manager:
		return "1=1", nil
	case a.NodeID != "":
		return "safe.id IN (SELECT x.file_id FROM " + table + " x JOIN auth_group ag ON ag.id = x.group_id" +
			" JOIN nodes n ON n.team_responsible = ag.role WHERE n.node_id = ?)", []any{a.NodeID}
	}
	var conds []string
	var args []any
	if a.UserID != nil {
		conds = append(conds, "safe.uploader = ?")
		args = append(args, *a.UserID)
	}
	if len(a.Groups) > 0 {
		conds = append(conds, "safe.id IN (SELECT x.file_id FROM "+table+" x JOIN auth_group ag ON ag.id = x.group_id"+
			" WHERE ag.role IN ("+Placeholders(len(a.Groups))+"))")
		for _, g := range a.Groups {
			args = append(args, g)
		}
	}
	if len(conds) == 0 {
		return "1=0", nil
	}
	return "(" + strings.Join(conds, " OR ") + ")", args
}

// safeRefCond selects a file by its id, or by its stored name, as the historical
// handlers accept both.
func safeRefCond(ref string) (string, []any) {
	if id, err := strconv.ParseInt(ref, 10, 64); err == nil {
		return "safe.id = ?", []any{id}
	}
	return "safe.uuid = ?", []any{ref}
}

// GetSafeFiles lists the safe files the caller may see: the one named by ref
// when set; the previous versions of the file historyOf when set, those whose
// stored name its safe_log records, as rest_get_safe_file_history does.
func (oDb *DB) GetSafeFiles(ctx context.Context, ref *string, historyOf *int64, a SafeAccess, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getSafeFiles: no select expressions")
	}
	cond, args := safeGroupCond("safe_team_publication", a)
	conds := []string{cond}
	if ref != nil {
		refCond, refArgs := safeRefCond(*ref)
		conds = append(conds, refCond)
		args = append(args, refArgs...)
	}
	if historyOf != nil {
		conds = append(conds, "safe.uuid IN (SELECT safe_log.uuid FROM safe_log WHERE safe_log.safe_id = ?)", "safe.id <> ?")
		args = append(args, *historyOf, *historyOf)
	}
	filterConds, filterArgs := p.FilterConditions()
	conds = append(conds, filterConds...)
	args = append(args, filterArgs...)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM safe LEFT JOIN auth_user ON auth_user.id = safe.uploader WHERE " + strings.Join(conds, " AND ")
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("safe.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getSafeFiles: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

const safeFileColumns = "safe.id, safe.uploader, COALESCE(safe.uploaded_from, ''), " +
	"DATE_FORMAT(safe.uploaded_date, '%Y-%m-%d %H:%i:%s'), COALESCE(safe.name, ''), COALESCE(safe.size, 0), " +
	"COALESCE(safe.uuid, ''), COALESCE(safe.md5, ''), COALESCE(safe.sha256, ''), safe.enc_version, " +
	"COALESCE(safe.enc_uuid, ''), COALESCE(safe.enc_key_id, ''), safe.enc_wrapped_key"

func scanSafeFile(row interface{ Scan(...any) error }) (*SafeFile, error) {
	var f SafeFile
	var uploader sql.NullInt64
	var encVersion sql.NullInt32
	err := row.Scan(&f.ID, &uploader, &f.UploadedFrom, &f.UploadedDate, &f.Name, &f.Size, &f.UUID, &f.MD5,
		&f.SHA256, &encVersion, &f.EncUUID, &f.EncKeyID, &f.WrappedKey)
	if err != nil {
		return nil, err
	}
	if uploader.Valid {
		f.Uploader = &uploader.Int64
	}
	if encVersion.Valid {
		v := int(encVersion.Int32)
		f.EncVersion = &v
	}
	return &f, nil
}

// SafeFileByRef returns a safe file by its id or stored name, nil when there is
// none. The access is not checked: see SafeFileAllowed.
func (oDb *DB) SafeFileByRef(ctx context.Context, ref string) (*SafeFile, error) {
	cond, args := safeRefCond(ref)
	f, err := scanSafeFile(oDb.DB.QueryRowContext(ctx, "SELECT "+safeFileColumns+" FROM safe WHERE "+cond+" LIMIT 1", args...))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("safeFileByRef: %w", err)
	}
	return f, nil
}

// SafeFileAllowed tells whether the caller may read a file (publication) or
// manage it (responsible, manage true).
func (oDb *DB) SafeFileAllowed(ctx context.Context, fileID int64, a SafeAccess, manage bool) (bool, error) {
	table := "safe_team_publication"
	if manage {
		table = "safe_team_responsible"
	}
	cond, args := safeGroupCond(table, a)
	var n int
	err := oDb.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM safe WHERE safe.id = ? AND "+cond,
		append([]any{fileID}, args...)...).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("safeFileAllowed: %w", err)
	}
	return n > 0, nil
}

// SafeFileContent is what a new version of a file sets: its stored name and
// digests, and its encryption, none for a plaintext file.
type SafeFileContent struct {
	Name         string
	UUID         string
	Size         int64
	MD5          string
	SHA256       string
	Uploader     *int64
	UploadedFrom string
	EncVersion   *int
	EncKeyID     string
	WrappedKey   []byte
}

func (c SafeFileContent) encArgs() []any {
	if c.EncVersion == nil {
		return []any{nil, nil, nil, nil}
	}
	return []any{*c.EncVersion, c.UUID, c.EncKeyID, c.WrappedKey}
}

// InsertSafeFile records a new file, linked to a responsible and a publication
// group when given, as lib_safe_upload() and lib_safe_file_create() do.
func (oDb *DB) InsertSafeFile(ctx context.Context, c SafeFileContent, groupID *int64) (int64, error) {
	tx, err := oDb.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("insertSafeFile: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	args := []any{c.Uploader, c.UploadedFrom, c.Name, c.Size, c.UUID, c.MD5, c.SHA256}
	res, err := tx.ExecContext(ctx, "INSERT INTO safe (uploader, uploaded_from, uploaded_date, name, size, uuid, md5,"+
		" sha256, enc_version, enc_uuid, enc_key_id, enc_wrapped_key) VALUES (?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		append(args, c.encArgs()...)...)
	if err != nil {
		return 0, fmt.Errorf("insertSafeFile: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insertSafeFile: %w", err)
	}
	if groupID != nil {
		for _, table := range []string{"safe_team_responsible", "safe_team_publication"} {
			if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" (file_id, group_id) VALUES (?, ?)", id, *groupID); err != nil {
				return 0, fmt.Errorf("insertSafeFile: %w", err)
			}
		}
	}
	return id, tx.Commit()
}

// UpdateSafeFileContent makes a new version of a file, as lib_safe_file_upload()
// does: the row takes the new content and keeps its id, which the safe://
// references name; the previous content becomes a new row, logged in safe_log,
// with the same responsibles and publications. Returns the id of that row, 0 for
// a file reference created empty (POST /safe), which had no content to keep.
func (oDb *DB) UpdateSafeFileContent(ctx context.Context, id int64, c SafeFileContent) (int64, error) {
	tx, err := oDb.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("updateSafeFileContent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	prev, err := scanSafeFile(tx.QueryRowContext(ctx, "SELECT "+safeFileColumns+" FROM safe WHERE safe.id = ? FOR UPDATE", id))
	if err != nil {
		return 0, fmt.Errorf("updateSafeFileContent: %w", err)
	}
	name := c.Name
	if name == "" {
		name = prev.Name
	}
	args := []any{c.Uploader, c.UploadedFrom, name, c.Size, c.UUID, c.MD5, c.SHA256}
	args = append(args, c.encArgs()...)
	if _, err := tx.ExecContext(ctx, "UPDATE safe SET uploader = ?, uploaded_from = ?, uploaded_date = NOW(), name = ?,"+
		" size = ?, uuid = ?, md5 = ?, sha256 = ?, enc_version = ?, enc_uuid = ?, enc_key_id = ?, enc_wrapped_key = ?"+
		" WHERE id = ?", append(args, id)...); err != nil {
		return 0, fmt.Errorf("updateSafeFileContent: %w", err)
	}
	if prev.UUID == "" {
		return 0, tx.Commit()
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO safe (uploader, uploaded_from, uploaded_date, name, size, uuid, md5,"+
		" sha256, enc_version, enc_uuid, enc_key_id, enc_wrapped_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		prev.Uploader, prev.UploadedFrom, prev.UploadedDate, prev.Name, prev.Size, prev.UUID, prev.MD5,
		nullString(prev.SHA256), prev.EncVersion, nullString(prev.EncUUID), nullString(prev.EncKeyID), prev.WrappedKey)
	if err != nil {
		return 0, fmt.Errorf("updateSafeFileContent: %w", err)
	}
	prevID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("updateSafeFileContent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO safe_log (safe_id, uuid) VALUES (?, ?)", id, prev.UUID); err != nil {
		return 0, fmt.Errorf("updateSafeFileContent: %w", err)
	}
	for _, table := range []string{"safe_team_responsible", "safe_team_publication"} {
		if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO "+table+" (file_id, group_id)"+
			" SELECT ?, group_id FROM "+table+" WHERE file_id = ?", prevID, id); err != nil {
			return 0, fmt.Errorf("updateSafeFileContent: %w", err)
		}
	}
	return prevID, tx.Commit()
}

// SetSafeFileEncryption records that a row now holds its file encrypted, the
// stored name unchanged: what the conversion of a plaintext file and a rewrap
// write. The row must still hold the stored name encrypted.
func (oDb *DB) SetSafeFileEncryption(ctx context.Context, id int64, uuid string, version int, keyID string, wrappedKey []byte, sha256 string) (bool, error) {
	res, err := oDb.DB.ExecContext(ctx, "UPDATE safe SET enc_version = ?, enc_uuid = ?, enc_key_id = ?, enc_wrapped_key = ?,"+
		" sha256 = COALESCE(NULLIF(?, ''), sha256) WHERE id = ? AND uuid = ?", version, uuid, keyID, wrappedKey, sha256, id, uuid)
	if err != nil {
		return false, fmt.Errorf("setSafeFileEncryption: %w", err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// SafeFilesToConvert lists the rows holding a plaintext file, oldest first.
func (oDb *DB) SafeFilesToConvert(ctx context.Context, limit int) ([]*SafeFile, error) {
	return oDb.safeFilesWhere(ctx, "safe.uuid <> '' AND (safe.enc_version IS NULL OR safe.enc_uuid IS NULL"+
		" OR safe.enc_uuid <> safe.uuid) ORDER BY safe.id LIMIT ?", limit)
}

// SafeFilesToRewrap lists the encrypted rows whose file key another master key
// than activeKeyID wraps.
func (oDb *DB) SafeFilesToRewrap(ctx context.Context, activeKeyID string, limit int) ([]*SafeFile, error) {
	return oDb.safeFilesWhere(ctx, "safe.enc_version IS NOT NULL AND safe.enc_uuid = safe.uuid"+
		" AND safe.enc_key_id <> ? ORDER BY safe.id LIMIT ?", activeKeyID, limit)
}

func (oDb *DB) safeFilesWhere(ctx context.Context, where string, args ...any) ([]*SafeFile, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT "+safeFileColumns+" FROM safe WHERE "+where, args...)
	if err != nil {
		return nil, fmt.Errorf("safeFiles: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []*SafeFile
	for rows.Next() {
		f, err := scanSafeFile(rows)
		if err != nil {
			return nil, fmt.Errorf("safeFiles: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// SetSafeFileName renames a file, its content unchanged.
func (oDb *DB) SetSafeFileName(ctx context.Context, id int64, name string) error {
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE safe SET name = ? WHERE id = ?", name, id); err != nil {
		return fmt.Errorf("setSafeFileName: %w", err)
	}
	return nil
}

// DeleteSafeFile removes a row with its responsibles and publications. The other
// versions of the file are rows of their own, left as they are.
func (oDb *DB) DeleteSafeFile(ctx context.Context, id int64) error {
	tx, err := oDb.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("deleteSafeFile: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		"DELETE FROM safe_team_responsible WHERE file_id = ?",
		"DELETE FROM safe_team_publication WHERE file_id = ?",
		"DELETE FROM safe_log WHERE safe_id = ?",
		"DELETE FROM safe WHERE id = ?",
	} {
		if _, err := tx.ExecContext(ctx, q, id); err != nil {
			return fmt.Errorf("deleteSafeFile: %w", err)
		}
	}
	return tx.Commit()
}

// SafeFileUsedElsewhere tells whether another row holds the same stored name: its
// file on disk is still needed.
func (oDb *DB) SafeFileUsedElsewhere(ctx context.Context, id int64, uuid string) (bool, error) {
	var n int
	if err := oDb.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM safe WHERE uuid = ? AND id <> ?", uuid, id).Scan(&n); err != nil {
		return false, fmt.Errorf("safeFileUsedElsewhere: %w", err)
	}
	return n > 0, nil
}

// SafeGroup is a group linked to a safe file, as publication or responsible.
type SafeGroup struct {
	ID   int64  `json:"id"`
	Role string `json:"role"`
}

func safeGroupTable(responsible bool) string {
	if responsible {
		return "safe_team_responsible"
	}
	return "safe_team_publication"
}

// SafeFileGroups lists the groups a file is published to, or made the
// responsibility of.
func (oDb *DB) SafeFileGroups(ctx context.Context, fileID int64, responsible bool) ([]SafeGroup, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT ag.id, ag.role FROM "+safeGroupTable(responsible)+" x"+
		" JOIN auth_group ag ON ag.id = x.group_id WHERE x.file_id = ? ORDER BY ag.role", fileID)
	if err != nil {
		return nil, fmt.Errorf("safeFileGroups: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []SafeGroup{}
	for rows.Next() {
		var g SafeGroup
		if err := rows.Scan(&g.ID, &g.Role); err != nil {
			return nil, fmt.Errorf("safeFileGroups: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// AddSafeFileGroup links a group to a file; false when it already was.
func (oDb *DB) AddSafeFileGroup(ctx context.Context, fileID, groupID int64, responsible bool) (bool, error) {
	res, err := oDb.DB.ExecContext(ctx, "INSERT IGNORE INTO "+safeGroupTable(responsible)+" (file_id, group_id) VALUES (?, ?)", fileID, groupID)
	if err != nil {
		return false, fmt.Errorf("addSafeFileGroup: %w", err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// RemoveSafeFileGroup unlinks a group from a file; false when it was not linked.
func (oDb *DB) RemoveSafeFileGroup(ctx context.Context, fileID, groupID int64, responsible bool) (bool, error) {
	res, err := oDb.DB.ExecContext(ctx, "DELETE FROM "+safeGroupTable(responsible)+" WHERE file_id = ? AND group_id = ?", fileID, groupID)
	if err != nil {
		return false, fmt.Errorf("removeSafeFileGroup: %w", err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// SafeDefaultGroupID is the group a new file is published to and made the
// responsibility of: for a node, its responsible team (auth_node_group_id());
// for a user, their primary group, else their private group, else their first
// organizational group (user_default_group_id()).
func (oDb *DB) SafeDefaultGroupID(ctx context.Context, a SafeAccess) (*int64, error) {
	var queries []struct {
		q    string
		args []any
	}
	switch {
	case a.NodeID != "":
		queries = append(queries, struct {
			q    string
			args []any
		}{"SELECT ag.id FROM nodes n JOIN auth_group ag ON ag.role = n.team_responsible WHERE n.node_id = ? LIMIT 1", []any{a.NodeID}})
	case a.UserID != nil:
		uid := *a.UserID
		for _, q := range []string{
			"SELECT ag.id FROM auth_membership am JOIN auth_group ag ON ag.id = am.group_id WHERE am.user_id = ? AND am.primary_group = 'T' LIMIT 1",
			"SELECT ag.id FROM auth_group ag WHERE ag.role = CONCAT('user_', ?) LIMIT 1",
			"SELECT ag.id FROM auth_membership am JOIN auth_group ag ON ag.id = am.group_id WHERE am.user_id = ?" +
				" AND ag.privilege = 'F' AND ag.role <> 'Everybody' ORDER BY ag.role LIMIT 1",
		} {
			queries = append(queries, struct {
				q    string
				args []any
			}{q, []any{uid}})
		}
	}
	for _, q := range queries {
		var id int64
		err := oDb.DB.QueryRowContext(ctx, q.q, q.args...).Scan(&id)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return nil, fmt.Errorf("safeDefaultGroupID: %w", err)
		}
		return &id, nil
	}
	return nil, nil
}

// SafeFileUsage is where a safe file is referenced: the services whose
// configuration names it, and the ruleset variables holding it, those of the
// rulesets published to the caller's groups, as rest_get_safe_file_usage does.
type SafeFileUsage struct {
	Services  []map[string]any `json:"services"`
	Variables []map[string]any `json:"variables"`
}

func (oDb *DB) GetSafeFileUsage(ctx context.Context, f *SafeFile, a SafeAccess) (*SafeFileUsage, error) {
	usage := &SafeFileUsage{Services: []map[string]any{}, Variables: []map[string]any{}}
	refs := []string{"%{safe://" + strconv.FormatInt(f.ID, 10) + "}%"}
	if f.UUID != "" {
		refs = append(refs, "%{safe://"+f.UUID+"}%")
	}
	svcQuery := "SELECT services.svcname, services.svc_id FROM services WHERE (" +
		strings.TrimSuffix(strings.Repeat("services.svc_config LIKE ? OR ", len(refs)), " OR ") + ")"
	svcArgs := make([]any, 0, len(refs))
	for _, r := range refs {
		svcArgs = append(svcArgs, r)
	}
	if !a.Manager {
		if len(a.Groups) == 0 {
			svcQuery += " AND 1=0"
		} else {
			svcQuery += " AND services.svc_app IN (SELECT apps.app FROM apps JOIN apps_responsibles ar ON ar.app_id = apps.id" +
				" JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN (" + Placeholders(len(a.Groups)) + "))"
			for _, g := range a.Groups {
				svcArgs = append(svcArgs, g)
			}
		}
	}
	rows, err := oDb.DB.QueryContext(ctx, svcQuery+" ORDER BY services.svcname", svcArgs...)
	if err != nil {
		return nil, fmt.Errorf("getSafeFileUsage: %w", err)
	}
	for rows.Next() {
		var name, id string
		if err := rows.Scan(&name, &id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("getSafeFileUsage: %w", err)
		}
		usage.Services = append(usage.Services, map[string]any{"svcname": name, "svc_id": id})
	}
	_ = rows.Close()

	varRefs := []any{"%safe://" + strconv.FormatInt(f.ID, 10) + "%"}
	varCond := "v.var_value LIKE ?"
	if f.UUID != "" {
		varRefs = append(varRefs, "%"+f.UUID+"%")
		varCond = "(" + varCond + " OR v.var_value LIKE ?)"
	}
	varQuery := "SELECT DISTINCT v.var_name, v.id, r.ruleset_name, r.id FROM comp_rulesets_variables v" +
		" JOIN comp_rulesets r ON r.id = v.ruleset_id WHERE " + varCond
	if !a.Manager {
		if len(a.Groups) == 0 {
			varQuery += " AND 1=0"
		} else {
			varQuery += " AND r.id IN (SELECT p.ruleset_id FROM comp_ruleset_team_publication p JOIN auth_group ag ON ag.id = p.group_id" +
				" WHERE ag.role IN (" + Placeholders(len(a.Groups)) + "))"
			for _, g := range a.Groups {
				varRefs = append(varRefs, g)
			}
		}
	}
	rows, err = oDb.DB.QueryContext(ctx, varQuery+" ORDER BY v.var_name", varRefs...)
	if err != nil {
		return nil, fmt.Errorf("getSafeFileUsage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var varName, rsetName string
		var varID, rsetID int64
		if err := rows.Scan(&varName, &varID, &rsetName, &rsetID); err != nil {
			return nil, fmt.Errorf("getSafeFileUsage: %w", err)
		}
		usage.Variables = append(usage.Variables, map[string]any{"var_name": varName, "id": varID, "ruleset_name": rsetName, "ruleset_id": rsetID})
	}
	return usage, rows.Err()
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
