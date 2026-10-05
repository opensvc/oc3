package cdb

import (
	"context"
	"fmt"
)

// SearchTags returns the tags whose name or exclusion pattern contains the LIKE
// pattern, those matching by name first, then by name, up
// to limit rows. Tags are readable by every user, as in the historical search.
func (oDb *DB) SearchTags(ctx context.Context, pattern string, limit int) ([]map[string]any, error) {
	rows, err := oDb.DB.QueryContext(ctx,
		`SELECT COALESCE(tag_id, ''), COALESCE(tag_name, ''), COALESCE(tag_exclude, '')
		FROM tags WHERE tag_name LIKE ? OR tag_exclude LIKE ?
		ORDER BY tag_name LIKE ? DESC, tag_name, id LIMIT ?`, pattern, pattern, pattern, limit)
	if err != nil {
		return nil, fmt.Errorf("searchTags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []map[string]any
	for rows.Next() {
		var tagID, name, exclude string
		if err := rows.Scan(&tagID, &name, &exclude); err != nil {
			return nil, fmt.Errorf("searchTags scan: %w", err)
		}
		out = append(out, map[string]any{"tag_id": tagID, "tag_name": name, "tag_exclude": exclude})
	}
	return out, rows.Err()
}
