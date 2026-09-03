package notification

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/outbound"
)

func TestProviderSenderUsesReliableHTTPClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/send" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		body, _ := io.ReadAll(request.Body)
		if len(body) == 0 {
			t.Fatal("empty provider payload")
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"message_id":"provider-1"}`))
	}))
	t.Cleanup(server.Close)
	client, err := outbound.NewHTTPClient("mail-provider", config.HTTPUpstream{BaseURL: server.URL, Timeout: time.Second, Retry: config.Retry{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sender := &ProviderSender{provider: "mail-primary", channel: "email", client: client, path: "/send"}
	result, err := sender.Send(t.Context(), Message{DeliveryID: "delivery-1", Channel: "email", Recipient: "a@example.com", Subject: "Hello", Content: "World"})
	if err != nil || result.MessageID != "provider-1" || result.Provider != "mail-primary" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
