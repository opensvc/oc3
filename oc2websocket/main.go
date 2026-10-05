// Package oc2websocket provides T that can publish events to
// opensvc collector v2 websocket publisher.
package oc2websocket

import (
	"crypto/hmac"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"
)

type (
	event struct {
		UUID uuid.UUID `json:"uuid"`
		Data []any     `json:"data"`
	}

	T struct {
		// URL is the url of opensvc collector v2 websocket publisher
		Url string
		// Key is the sign key for pushed messages
		Key []byte
	}
)

func (s *T) pub(e *event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	h := hmac.New(md5.New, s.Key)
	if _, err := h.Write(b); err != nil {
		return err
	}
	sum := h.Sum(nil)
	signature := hex.EncodeToString(sum)

	params := url.Values{}
	params.Add("message", string(b))
	params.Add("signature", signature)
	params.Add("group", "generic")
	resp, err := http.PostForm(s.Url, params)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if _, err := io.ReadAll(resp.Body); err != nil {
		return err
	}
	return nil
}

// RegisterToken announces a one-time token to the messenger: the websocket
// client presenting it is let in when the messenger requires tokens, as the
// historical collector did for its comet server. The token is signed like an
// event.
func (s *T) RegisterToken(token string) error {
	h := hmac.New(md5.New, s.Key)
	if _, err := h.Write([]byte(token)); err != nil {
		return err
	}
	params := url.Values{}
	params.Add("message", token)
	params.Add("signature", hex.EncodeToString(h.Sum(nil)))
	endpoint, err := url.JoinPath(s.Url, "token")
	if err != nil {
		return err
	}
	resp, err := http.PostForm(endpoint, params)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("messenger refused the token: %s", resp.Status)
	}
	return nil
}

// EventPublish publish a new event to opensvc collector v2 websocket publisher
func (s *T) EventPublish(evName string, data map[string]any) error {
	if data == nil {
		data = make(map[string]any)
	}
	data["event"] = evName
	data["version"] = "3.0.0"
	ev := &event{Data: []any{data}}
	ev.UUID, _ = uuid.NewUUID()
	return s.pub(ev)
}
