package serverhandlers

import (
	"net/http"
	"strings"
	"testing"
)

// TestSafeNodeManagesNoFile checks a node is refused every change of a safe
// file, before any lookup: the Api has no database.
func TestSafeNodeManagesNoFile(t *testing.T) {
	a := &Api{}
	for what, call := range map[string]func(a *Api) (int, string){
		"publish": func(a *Api) (int, string) {
			c, rec := aiContext(http.MethodPost, "/safe/1/publications/Everybody", "")
			nodeAuth(c)
			_ = a.PostSafeFilePublication(c, "1", "Everybody")
			return rec.Code, rec.Body.String()
		},
		"add a responsible": func(a *Api) (int, string) {
			c, rec := aiContext(http.MethodPost, "/safe/1/responsibles/g", "")
			nodeAuth(c)
			_ = a.PostSafeFileResponsible(c, "1", "g")
			return rec.Code, rec.Body.String()
		},
		"rename": func(a *Api) (int, string) {
			c, rec := aiContext(http.MethodPost, "/safe/1", `{"name":"x"}`)
			nodeAuth(c)
			_ = a.PostSafeFileName(c, "1")
			return rec.Code, rec.Body.String()
		},
		"upload a version": func(a *Api) (int, string) {
			c, rec := aiContext(http.MethodPost, "/safe/1/upload", "")
			nodeAuth(c)
			_ = a.PostSafeFileUpload(c, "1")
			return rec.Code, rec.Body.String()
		},
		"delete": func(a *Api) (int, string) {
			c, rec := aiContext(http.MethodDelete, "/safe/1", "")
			nodeAuth(c)
			_ = a.DeleteSafeFile(c, "1")
			return rec.Code, rec.Body.String()
		},
	} {
		if code, body := call(a); code != http.StatusForbidden || !strings.Contains(body, "a node can not manage") {
			t.Errorf("%s: %d %s", what, code, body)
		}
	}
}

// nodeAuth makes a test context the one of a request of node n1.
func nodeAuth(c interface{ Set(string, any) }) {
	c.Set(XAuthMode, AuthModeNode)
	c.Set(XNodeID, "n1")
	c.Set(XNodename, "node1")
}
