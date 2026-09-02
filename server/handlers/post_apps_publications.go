package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// PostAppsPublications handles POST /apps_publications (bulk).
func (a *Api) PostAppsPublications(c echo.Context) error {
	appId, groupId, err := decodeAppPublicationKeys(c)
	if err != nil {
		return err
	}
	return a.postAppPublication(c, "PostAppsPublications", appId, groupId)
}

func decodeAppPublicationKeys(c echo.Context) (string, string, error) {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return "", "", JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawAppID, ok := body["app_id"]
	if !ok {
		return "", "", JSONProblemf(c, http.StatusBadRequest, "The 'app_id' key is mandatory")
	}
	rawGroupID, ok := body["group_id"]
	if !ok {
		return "", "", JSONProblemf(c, http.StatusBadRequest, "The 'group_id' key is mandatory")
	}
	return fmt.Sprintf("%v", rawAppID), fmt.Sprintf("%v", rawGroupID), nil
}
