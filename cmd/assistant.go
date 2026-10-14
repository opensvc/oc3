package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/viper"

	handlers "github.com/opensvc/oc3/server/handlers"
)

// newAssistant reads the AI agent the assistant routes are relayed to, nil when
// none is configured. Both its base URL and the path of the collector's backend
// come from the configuration, neither has a default:
//
//	server:
//	  assistant:
//	    base_url: https://agent.example.com:8090
//	    path: /v1/oc3
//	    timeout: 30s          # the answers in one piece; the streamed turns run
//	                          # as long as the browser waits for them
//	    ca_file: /etc/oc3/agent-ca.pem   # when the agent's certificate is not
//	                                     # signed by a public authority
func newAssistant(s string) (*handlers.AssistantConfig, error) {
	k := s + ".assistant."
	viper.SetDefault(k+"timeout", "30s")
	base := strings.TrimSpace(viper.GetString(k + "base_url"))
	path := strings.TrimSpace(viper.GetString(k + "path"))
	if base == "" && path == "" {
		return nil, nil
	}
	if base == "" || path == "" {
		return nil, fmt.Errorf("%sbase_url and %spath go together: set both, or neither", k, k)
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("%sbase_url: %q is not an http(s) URL without query, fragment nor credentials", k, base)
	}
	if u.Scheme == "http" && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" {
		// The requests carry the users' access tokens.
		return nil, fmt.Errorf("%sbase_url: the access tokens are not sent over plain http, use https", k)
	}
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "..") || strings.ContainsAny(path, "?#") {
		return nil, fmt.Errorf("%spath: %q is not an absolute path", k, path)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile := viper.GetString(k + "ca_file"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("%sca_file: %w", k, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%sca_file: no PEM certificate", k)
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	timeout := viper.GetDuration(k + "timeout")
	// The agent does not redirect; a redirect would carry the token elsewhere.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	slog.Info("assistant: relayed to " + u.Redacted() + " path " + path)
	return &handlers.AssistantConfig{
		BaseURL: u,
		Path:    path,
		Client:  &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: noRedirect},
		Stream:  &http.Client{Transport: transport, CheckRedirect: noRedirect},
	}, nil
}
