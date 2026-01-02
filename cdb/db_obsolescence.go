package cdb

import (
	"context"
	"fmt"
)

func (oDb *DB) StatObsolescenceHW(ctx context.Context) error {
	query := `INSERT IGNORE INTO obsolescence (
			obs_type,
			obs_name,
			obs_warn_date_updated_by,
			obs_warn_date_updated,
			obs_alert_date_updated_by,
			obs_alert_date_updated
		     )
		     SELECT "hw", model, "collector", NOW(), "collector", NOW()
		     FROM nodes
		     WHERE model != ''
		     GROUP BY model
            `
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatObsolescenceOS(ctx context.Context) error {
	query := `INSERT IGNORE INTO obsolescence (
			obs_type,
			obs_name,
			obs_warn_date_updated_by,
			obs_warn_date_updated,
			obs_alert_date_updated_by,
			obs_alert_date_updated
		     )
		     SELECT "os", os_concat, "collector", NOW(), "collector", NOW()
		     FROM nodes
		     WHERE os_concat != ''
		     GROUP BY os_concat
                    `
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	return nil
}

// PurgeAlertsObsWithout deletes dashboard "obsolescence date not set" alerts if
// the node model or operating system is no longer the one referenced in the alert.
func (oDb *DB) PurgeAlertsObsWithout(ctx context.Context) error {
	query := `DELETE d FROM dashboard d
		  JOIN nodes n ON d.node_id = n.node_id
		  WHERE
		    d.dash_type IN ("hardware obsolescence alert date not set", "hardware obsolescence warning date not set") AND
		    d.dash_dict != JSON_OBJECT('o', n.model)
	         `
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	query = `DELETE d FROM dashboard d
                 JOIN nodes n ON d.node_id=n.node_id
                 WHERE
		   d.dash_type IN ("os obsolescence alert date not set", "os obsolescence warning date not set") AND
                   d.dash_dict != JSON_OBJECT('o', CONCAT(n.os_name, " ", n.os_vendor, " ", n.os_release))
	        `
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	query = `DELETE FROM dashboard
                 WHERE
		   dash_type IN ("hardware obsolescence alert date not set", "hardware obsolescence warning date not set") AND
                   dash_dict IN (
                     SELECT JSON_OBJECT('o', obs_name)
                     FROM obsolescence
                     WHERE
                       obs_warn_date IS NOT NULL AND
                       obs_type = "hw"
                     )
	        `
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	query = `DELETE from dashboard
                 WHERE
		   dash_type IN ("hardware os alert date not set", "os obsolescence warning date not set") AND
                   dash_dict IN (
                     SELECT JSON_OBJECT('o', obs_name)
                     FROM obsolescence
                     WHERE
                       obs_warn_date IS NOT NULL AND
                       obs_type = "os"
                     )
	       `
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateNodesObsolescence(ctx context.Context) error {
	query := `
		UPDATE nodes n
		JOIN obsolescence o ON
		    (o.obs_type = 'hw' AND n.model = o.obs_name) OR
		    (o.obs_type = 'os' AND n.os_concat = o.obs_name)
		SET
		    n.hw_obs_warn_date = CASE
					    WHEN o.obs_type = 'hw' THEN o.obs_warn_date
					    ELSE n.hw_obs_warn_date
					 END,
		    n.hw_obs_alert_date = CASE
					     WHEN o.obs_type = 'hw' THEN o.obs_alert_date
					     ELSE n.hw_obs_alert_date
					  END,
		    n.os_obs_warn_date = CASE
					    WHEN o.obs_type = 'os' THEN o.obs_warn_date
					    ELSE n.os_obs_warn_date
					 END,
		    n.os_obs_alert_date = CASE
					     WHEN o.obs_type = 'os' THEN o.obs_alert_date
					     ELSE n.os_obs_alert_date
					  END
		WHERE o.id > 0`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update node obsolescence data failed: %w", err)
	}
	return nil
}
