package messenger

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

type CmdComet struct {
	Port         string
	Address      string
	Key          string
	RequireToken bool
	KeyFile      string
	CertFile     string
}

var (
	listeners = make(map[string][]*Client)
	names     = make(map[*Client]string)
	tokens    = make(map[string]*Client)
	hmacKey   string
	useTokens bool
	mu        sync.RWMutex
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

type (
	Client struct {
		conn  *websocket.Conn
		group string
		token string
		name  string
	}
)

func postHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if hmacKey != "" && r.FormValue("signature") == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	message := r.FormValue("message")
	if message == "" {
		http.Error(w, "Missing message", http.StatusBadRequest)
		return
	}

	group := r.FormValue("group")
	if group == "" {
		group = "default"
	}

	slog.Debug(fmt.Sprintf("MESSAGE to %s:%s", group, message))

	if hmacKey != "" {
		signature := r.FormValue("signature")
		h := hmac.New(md5.New, []byte(hmacKey))
		h.Write([]byte(message))
		expectedSig := hex.EncodeToString(h.Sum(nil))

		if signature != expectedSig {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	mu.RLock()
	clients := listeners[group]
	mu.RUnlock()

	for _, client := range clients {
		if err := client.conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			slog.Warn(fmt.Sprintf("Error writing to client: %v", err))
		}
	}

	w.WriteHeader(http.StatusOK)
}

func tokenHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if hmacKey != "" && r.FormValue("message") == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	message := r.FormValue("message")
	if message == "" {
		http.Error(w, "Missing message", http.StatusBadRequest)
		return
	}

	if hmacKey != "" {
		signature := r.FormValue("signature")
		h := hmac.New(md5.New, []byte(hmacKey))
		h.Write([]byte(message))
		expectedSig := hex.EncodeToString(h.Sum(nil))

		if signature != expectedSig {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	mu.Lock()
	tokens[message] = nil
	mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func distributeHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn(fmt.Sprintf("Error upgrading connection: %v", err))
		return
	}

	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/realtime/"), "/")

	group := "default"
	token := "none"
	name := "anonymous"

	if len(pathParts) > 0 && pathParts[0] != "" {
		group = pathParts[0]
	}
	if len(pathParts) > 1 && pathParts[1] != "" {
		token = pathParts[1]
	}
	if len(pathParts) > 2 && pathParts[2] != "" {
		name = pathParts[2]
	}

	client := &Client{
		conn:  conn,
		group: group,
		token: token,
		name:  name,
	}

	if useTokens {
		mu.RLock()
		tokenClient, exists := tokens[token]
		mu.RUnlock()

		if !exists || tokenClient != nil {
			conn.Close()
			return
		}

		mu.Lock()
		tokens[token] = client
		mu.Unlock()
	}

	mu.Lock()
	if listeners[group] == nil {
		listeners[group] = []*Client{}
	}

	for _, existingClient := range listeners[group] {
		if err := existingClient.conn.WriteMessage(websocket.TextMessage, []byte("+"+name)); err != nil {
			slog.Warn(fmt.Sprintf("Error notifying client: %v", err))
		}
	}

	listeners[group] = append(listeners[group], client)
	names[client] = name
	mu.Unlock()

	userAgent := r.Header.Get("User-Agent")

	slog.Info(fmt.Sprintf("CONNECT %s to %s", userAgent, group))
	slog.Debug(fmt.Sprintf("CONNECT %s to %s", userAgent, group))

	defer func() {
		mu.Lock()
		if clients, ok := listeners[group]; ok {
			for i, c := range clients {
				if c == client {
					listeners[group] = append(clients[:i], clients[i+1:]...)
					break
				}
			}
		}
		delete(names, client)
		mu.Unlock()

		conn.Close()
		slog.Info(fmt.Sprintf("DISCONNECT %s from %s", group, userAgent))
		slog.Debug(fmt.Sprintf("DISCONNECT %s from %s", group, userAgent))
		for _, existingClient := range listeners[group] {
			if err := existingClient.conn.WriteMessage(websocket.TextMessage, []byte("-"+name)); err != nil {
				slog.Warn(fmt.Sprintf("Error notifying client: %v", err))
			}
		}
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			slog.Warn(fmt.Sprintf("Error reading from client: %v", err))
			break
		}
	}
}

func (c *CmdComet) Run() error {
	hmacKey = c.Key
	useTokens = c.RequireToken

	http.HandleFunc("/", postHandler)
	http.HandleFunc("/token", tokenHandler)
	http.HandleFunc("/realtime/", distributeHandler)

	addr := fmt.Sprintf("%s:%s", c.Address, c.Port)

	if c.KeyFile != "" && c.CertFile != "" {
		slog.Debug(fmt.Sprintf("Starting HTTPS server on %s", addr))

		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			slog.Error(fmt.Sprintf("Error loading certificates: %v", err))
			return err
		}

		server := &http.Server{
			Addr: addr,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
			},
		}

		if err := server.ListenAndServeTLS("", ""); err != nil {
			slog.Error(fmt.Sprintf("Error starting HTTPS server: %v", err))
			return err
		}
	} else {
		slog.Debug(fmt.Sprintf("Starting HTTP server on %s", addr))
		if err := http.ListenAndServe(addr, nil); err != nil {
			slog.Error(fmt.Sprintf("Error starting HTTP server: %s", err))
			return err
		}
	}
	return nil
}
