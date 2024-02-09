package mariadb

type (
	Mappings []Mapping
	Mapping  struct {
		// To is the table column name
		To string

		// From is the loaded list element key name
		From string
	}
)

func NewNaturalMapping(s string) Mapping {
	return Mapping{
		To:   s,
		From: s,
	}
}

func NewMapping(to, from string) Mapping {
	return Mapping{
		To:   to,
		From: from,
	}
}
