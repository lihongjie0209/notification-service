package httptransport

import (
	"encoding/json"
	"testing"

	notificationdomain "github.com/lihongjie0209/notification-service/internal/notification"
)

func TestDeliveryResponsePreservesVariablesJSON(t *testing.T) {
	response := deliveryResponse(notificationdomain.Delivery{ID: "delivery-1", Variables: []byte(`{"name":"Alice"}`)})
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	variables, ok := body["variables"].(map[string]any)
	if !ok || variables["name"] != "Alice" {
		t.Fatalf("variables = %#v, want JSON object", body["variables"])
	}
}
