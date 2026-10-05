package serverhandlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestDecodeAppGroupKeysDenies(t *testing.T) {
	for body, want := range map[string]string{
		"not json":        "invalid character",
		`{}`:              "'app_id' key is mandatory",
		`{"app_id":"a1"}`: "'group_id' key is mandatory",
	} {
		req := httptest.NewRequest(http.MethodPost, "/apps_publications", strings.NewReader(body))
		rec := httptest.NewRecorder()
		c := echo.New().NewContext(req, rec)
		_, _, err := decodeAppGroupKeys(c)
		// The caller stops on a non-nil error: errRequestDenied, the 400 being sent.
		if !errors.Is(err, errRequestDenied) {
			t.Errorf("%s: error %v, want errRequestDenied", body, err)
		}
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s: response %d %s", body, rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/apps_publications", strings.NewReader(`{"app_id":"a1","group_id":7}`))
	c := echo.New().NewContext(req, httptest.NewRecorder())
	if app, group, err := decodeAppGroupKeys(c); err != nil || app != "a1" || group != "7" {
		t.Errorf("valid body: %q %q %v", app, group, err)
	}
}
