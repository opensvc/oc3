// Package oc2websocket provides T that can publish events to
// opensvc collector v2 websocket publisher.
package oc2websocket

import (
	"crypto/hmac"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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
