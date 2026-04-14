package schema

// Table represents a database table.
type Table struct {
	Name string
}

// Col represents a column in a table.
type Col struct {
	T        *Table
	Name     string
	Nullable bool
	// Ref is set in relations.go for FK relationships.
	// It points to the referenced column (e.g. NodesAppID.Ref = AppsID).
	Ref *Col
}

// Qualified returns "table.column" for use in SQL queries.
func (c *Col) Qualified() string {
	return c.T.Name + "." + c.Name
}
