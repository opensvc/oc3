package serverhandlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/xauth"
)

// fakeAgent records what reaches it and answers as the agent does.
type fakeAgent struct {
	method, path, query, auth, contentType, cookie, extra, body string
}

func newFakeAgent(t *testing.T, seen *fakeAgent) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		*seen = fakeAgent{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, auth: r.Header.Get("Authorization"),
			contentType: r.Header.Get("Content-Type"), cookie: r.Header.Get("Cookie"), extra: r.Header.Get("X-Extra"), body: string(b)}
		switch {
		case strings.HasSuffix(r.URL.Path, "/turns"):
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: text_delta\ndata: {\"type\":\"text_delta\",\"text_delta\":\"Hello\"}\n\n")
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, "event: completed\ndata: {\"type\":\"completed\"}\n\n")
		case strings.HasSuffix(r.URL.Path, "/missing"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":"conversation_not_found","message":"no such conversation"}}`)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"conversations":[]}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func assistantAPI(t *testing.T, agentURL string) *Api {
	t.Helper()
	u, _ := url.Parse(agentURL + "/base")
	return &Api{Assistant: &AssistantConfig{BaseURL: u, Path: "/v1/oc3", Client: http.DefaultClient, Stream: http.DefaultClient}}
}

// assistantRequest builds a request of a user signed in with source, carrying a
// bearer token, a cookie and another header, none of which but the token's
// value may reach the agent.
func assistantRequest(method, target, body, source string) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer user-access-token")
	req.Header.Set("Cookie", "oc3_session=secret-session")
	req.Header.Set("X-Extra", "do-not-forward")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	ext := make(auth.Extensions)
	ext.Set(xauth.XAuthSource, source)
	c.Set("user", auth.NewUserInfo("alice@example.com", "7", nil, ext))
	return c, rec
}

func TestAssistantRelay(t *testing.T) {
	var seen fakeAgent
	agent := newFakeAgent(t, &seen)
	a := assistantAPI(t, agent.URL)

	c, rec := assistantRequest(http.MethodGet, "/api/assistant/conversations/c1/messages?limit=20", "", xauth.AuthSourceBearer)
	limit := 20
	if err := a.GetAssistantMessages(c, "c1", server.GetAssistantMessagesParams{Limit: &limit}); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || seen.path != "/base/v1/oc3/conversations/c1/messages" || seen.query != "limit=20" {
		t.Fatalf("relayed to %s?%s, answered %d", seen.path, seen.query, rec.Code)
	}
	if seen.auth != "Bearer user-access-token" || seen.cookie != "" || seen.extra != "" {
		t.Fatalf("forwarded headers: auth %q cookie %q extra %q", seen.auth, seen.cookie, seen.extra)
	}

	// The agent's errors come back as they are.
	c, rec = assistantRequest(http.MethodGet, "/", "", xauth.AuthSourceBearer)
	if err := a.GetAssistantConversation(c, "missing"); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "conversation_not_found") {
		t.Fatalf("error: %d %s", rec.Code, rec.Body.String())
	}

	// An id that is not one never reaches the agent.
	for _, bad := range []string{"../../admin", "a/b", "a%2Fb", "", "a?x=1"} {
		seen = fakeAgent{}
		c, rec = assistantRequest(http.MethodGet, "/", "", xauth.AuthSourceBearer)
		_ = a.GetAssistantConversation(c, bad)
		if rec.Code != http.StatusBadRequest || seen.method != "" {
			t.Errorf("id %q: %d, agent called: %v", bad, rec.Code, seen.method != "")
		}
	}

	// A turn streams, its body relayed.
	c, rec = assistantRequest(http.MethodPost, "/", `{"prompt":"hi"}`, xauth.AuthSourceBearer)
	if err := a.PostAssistantTurn(c, "c1"); err != nil {
		t.Fatal(err)
	}
	if seen.body != `{"prompt":"hi"}` || seen.contentType != "application/json" || seen.method != http.MethodPost {
		t.Fatalf("turn request: %+v", seen)
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(rec.Body.String(), "event: completed") ||
		!strings.Contains(rec.Body.String(), `"text_delta":"Hello"`) {
		t.Fatalf("stream: %q %q", rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestAssistantRefusals(t *testing.T) {
	var seen fakeAgent
	agent := newFakeAgent(t, &seen)

	check := func(name string, a *Api, c echo.Context, rec *httptest.ResponseRecorder, status int, code string) {
		t.Helper()
		seen = fakeAgent{}
		if err := a.GetAssistantConversations(c); err != nil {
			t.Fatal(err)
		}
		if rec.Code != status || !strings.Contains(rec.Body.String(), code) || seen.method != "" {
			t.Errorf("%s: %d %s, agent called: %v", name, rec.Code, rec.Body.String(), seen.method != "")
		}
	}
	c, rec := assistantRequest(http.MethodGet, "/", "", xauth.AuthSourceBearer)
	check("not configured", &Api{}, c, rec, http.StatusNotFound, "assistant_disabled")
	c, rec = assistantRequest(http.MethodGet, "/", "", xauth.AuthSourceBasic)
	check("password sign-in", assistantAPI(t, agent.URL), c, rec, http.StatusUnauthorized, "assistant_token_unavailable")
	c, rec = assistantRequest(http.MethodGet, "/", "", xauth.AuthSourceBearer)
	c.Set(XImpersonator, auth.NewUserInfo("manager@example.com", "1", nil, nil))
	check("impersonating", assistantAPI(t, agent.URL), c, rec, http.StatusForbidden, "assistant_forbidden")
}
