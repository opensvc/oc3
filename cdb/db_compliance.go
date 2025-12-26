package cdb

import (
	"context"
)

func (oDb *DB) PurgeCompModulesetsNodes(ctx context.Context) error {
	var query = `DELETE
		FROM comp_node_moduleset
		WHERE
		  node_id NOT IN (
                    SELECT DISTINCT node_id FROM nodes
                   )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_node_moduleset")
	}
	return nil
}

func (oDb *DB) PurgeCompRulesetsNodes(ctx context.Context) error {
	var query = `DELETE
		FROM comp_rulesets_nodes
		WHERE
		  node_id NOT IN (
                    SELECT DISTINCT node_id FROM nodes
                   )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_rulesets_nodes")
	}
	return nil
}

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

func (oDb *DB) PurgeCompModulesetsServices(ctx context.Context) error {
	var query = `DELETE
		FROM comp_modulesets_services
		WHERE
		  svc_id NOT IN (
                    SELECT DISTINCT svc_id FROM svcmon
                   )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_modulesets_services")
	}
	return nil
}

// purge entries older than 30 days
func (oDb *DB) PurgeCompStatusOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM comp_status
		WHERE
		  run_date < DATE_SUB(NOW(), INTERVAL 31 DAY)`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge svc compliance status for deleted services
func (oDb *DB) PurgeCompStatusSvcOrphans(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
               svc_id != "" and
               svc_id NOT IN (
                 SELECT DISTINCT svc_id FROM svcmon
	       )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge node compliance status for deleted nodes
func (oDb *DB) PurgeCompStatusNodeOrphans(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
               node_id != "" and
               node_id NOT IN (
                 SELECT DISTINCT node_id FROM nodes
	       )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge compliance status older than 7 days for modules in no moduleset, ie not schedulable
func (oDb *DB) PurgeCompStatusModulesetOrphans(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
	       run_date < DATE_SUB(NOW(), INTERVAL 7 DAY) AND
               run_module NOT IN (
                 SELECT modset_mod_name FROM comp_moduleset_modules
	       )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge node compliance status older than 7 days for unattached modules
func (oDb *DB) PurgeCompStatusNodeUnattached(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
	       run_date < DATE_SUB(NOW(), INTERVAL 7 DAY) AND
	       svc_id = "" AND
               run_module NOT IN (
                 SELECT modset_mod_name
                 FROM comp_moduleset_modules
                 WHERE modset_id IN (
                   SELECT modset_id
                   FROM comp_node_moduleset
                 )
	       )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge svc compliance status older than 7 days for unattached modules
func (oDb *DB) PurgeCompStatusSvcUnattached(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
	       run_date < DATE_SUB(NOW(), INTERVAL 7 DAY) AND
	       svc_id = "" AND
               run_module NOT IN (
                 SELECT modset_mod_name
                 FROM comp_moduleset_modules
                 WHERE modset_id IN (
                   SELECT modset_id
                   FROM comp_modulesets_services
                 )
	       )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}
