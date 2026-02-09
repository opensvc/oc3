package cdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type Tag struct {
	ID         int    `json:"id"`
	TagName    string `json:"tag_name"`
	TagCreated string `json:"tag_created"`
	TagExclude string `json:"tag_exclude"`
	TagData    any    `json:"tag_data"`
	TagID      string `json:"tag_id"`
}

// GetTags returns all tags with id > 0, or a specific tag if tagID is provided
func (oDb *DB) GetTags(ctx context.Context, tagID *int) ([]Tag, error) {
	query := `
		SELECT id, tag_name, tag_created, tag_exclude, tag_data, tag_id
		FROM tags
		WHERE id > 0
	`
	var args []interface{}

	if tagID != nil {
		query += " AND id = ?"
		args = append(args, *tagID)
	}

	query += " ORDER BY tag_name"

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getTags: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tags []Tag
	for rows.Next() {
		var tag Tag
		var tagCreated, tagExclude, tagData, tagID sql.NullString
		if err := rows.Scan(&tag.ID, &tag.TagName, &tagCreated, &tagExclude, &tagData, &tagID); err != nil {
			return nil, fmt.Errorf("getTags scan: %w", err)
		}
		if tagCreated.Valid {
			tag.TagCreated = tagCreated.String
		}
		if tagExclude.Valid {
			tag.TagExclude = tagExclude.String
		}
		if tagData.Valid && tagData.String != "" {
			// Try to parse as JSON
			var parsed any
			if err := json.Unmarshal([]byte(tagData.String), &parsed); err == nil {
				tag.TagData = parsed
			} else {
				// If not valid JSON, keep as string
				tag.TagData = tagData.String
			}
		}
		if tagID.Valid {
			tag.TagID = tagID.String
		}
		tags = append(tags, tag)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getTags rows: %w", err)
	}

	return tags, nil
}
