package cdb

import (
	"context"
)

func (oDb *DB) PurgeCompRulesetsServices(ctx context.Context) error {
	var query = `DELETE
		FROM comp_rulesets_services
		WHERE
		  svc_id NOT IN (
                    SELECT DISTINCT svc_id FROM svcmon
                   )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_rulesets_services")
	}
	return nil
}
