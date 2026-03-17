package serverhandlers

type propMapping struct {
	Available []string
	Blacklist map[string]struct{}
}

var propsMapping = map[string]propMapping{
	"tag": {
		Available: []string{"id", "tag_name", "tag_created", "tag_exclude", "tag_data", "tag_id"},
		Blacklist: map[string]struct{}{"id": {}},
	},
	"moduleset": {
		Available: []string{"id", "modset_name", "modset_author", "modset_updated"},
	},
	"ruleset": {
		Available: []string{"id", "ruleset_name", "ruleset_type", "ruleset_public"},
	},
}
