package worker

import (
	"context"
)

func (oDb *opensvcDB) updateClustersData(ctx context.Context, clusterName, clusterID, data string) error {
	// TODO: verify if still needed, we can't assert things here
	// +--------------+--------------+------+-----+---------+----------------+
	// | Field        | Type         | Null | Key | Default | Extra          |
	// +--------------+--------------+------+-----+---------+----------------+
	// | id           | int(11)      | NO   | PRI | NULL    | auto_increment |
	// | cluster_id   | char(36)     | YES  | UNI |         |                |
	// | cluster_name | varchar(128) | NO   |     | NULL    |                |
	// | cluster_data | longtext     | YES  |     | NULL    |                |
	// +--------------+--------------+------+-----+---------+----------------+
	const (
		query = `INSERT INTO clusters (cluster_name, cluster_id, cluster_data)
			VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE cluster_name = ?, cluster_data = ?`
	)
	_, err := oDb.db.ExecContext(ctx, query, clusterName, clusterID, data, clusterName, data)
	return err
}
