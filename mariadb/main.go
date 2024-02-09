package mariadb

type (
	Columns []Column
	Column  struct {
		Name  string
		Alias []string
	}
)
