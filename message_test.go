package Redelay

import (
	"encoding/json"
	"testing"
)

type itPayload struct {
	RoomId string `json:"roomId"`
	Count  int64  `json:"count"`
}

func TestMessage_MarshallGeneratesMsgId(t *testing.T) {
	m := &Message[string]{Data: "hello"}

	first := m.Marshall()
	if m.MsgId == "" {
		t.Fatalf("MsgId should be generated, got message: %s", first)
	}
	if len(m.MsgId) != 32 {
		t.Fatalf("MsgId expected 32 hex chars, got %d: %s", len(m.MsgId), m.MsgId)
	}

	second := m.Marshall()
	if m.MsgId != "" && second != first {
		t.Fatalf("Marshall should be idempotent, first: %s, second: %s", first, second)
	}

	raw := map[string]any{}
	if err := json.Unmarshal([]byte(first), &raw); err != nil {
		t.Fatalf("unmarshal %s failed: %s", first, err)
	}
	if raw["msgId"] != m.MsgId {
		t.Fatalf("msgId field expected %s, got %+v", m.MsgId, raw["msgId"])
	}
	if raw["data"] != "hello" {
		t.Fatalf("data field expected hello, got %+v", raw["data"])
	}
	if _, ok := raw["opId"]; ok {
		t.Fatalf("opId should not be serialized, message: %s", first)
	}
	if len(raw) != 2 {
		t.Fatalf("message should serialize exactly msgId and data, got %+v", raw)
	}
}

func TestMessage_MarshallKeepsProvidedMsgId(t *testing.T) {
	m := &Message[string]{MsgId: "custom-msg-id", Data: "x"}

	got := m.Marshall()
	if m.MsgId != "custom-msg-id" {
		t.Fatalf("provided MsgId was overwritten: %s", m.MsgId)
	}

	raw := map[string]any{}
	if err := json.Unmarshal([]byte(got), &raw); err != nil {
		t.Fatalf("unmarshal %s failed: %s", got, err)
	}
	if raw["msgId"] != "custom-msg-id" {
		t.Fatalf("msgId expected custom-msg-id, got %+v", raw["msgId"])
	}
}

func TestMessage_MarshallGeneratesUniqueIds(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		m := &Message[string]{Data: "same-payload"}
		m.Marshall()
		if _, dup := seen[m.MsgId]; dup {
			t.Fatalf("duplicate MsgId generated: %s", m.MsgId)
		}
		seen[m.MsgId] = struct{}{}
	}
	if len(seen) != 1000 {
		t.Fatalf("expected 1000 unique MsgId, got %d", len(seen))
	}
}

func TestMessage_RoundTripGenericTypes(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		origin := &Message[itPayload]{MsgId: "id-struct", Data: itPayload{RoomId: "room-1", Count: 42}}
		s := origin.Marshall()

		decoded := &Message[itPayload]{}
		if err := json.Unmarshal([]byte(s), decoded); err != nil {
			t.Fatalf("unmarshal %s failed: %s", s, err)
		}
		if decoded.MsgId != "id-struct" || decoded.Data.RoomId != "room-1" || decoded.Data.Count != 42 {
			t.Fatalf("round trip mismatch, got %+v", decoded)
		}
	})

	t.Run("slice", func(t *testing.T) {
		origin := &Message[[]int64]{MsgId: "id-slice", Data: []int64{1, 2, 3}}
		s := origin.Marshall()

		decoded := &Message[[]int64]{}
		if err := json.Unmarshal([]byte(s), decoded); err != nil {
			t.Fatalf("unmarshal %s failed: %s", s, err)
		}
		if len(decoded.Data) != 3 || decoded.Data[2] != 3 {
			t.Fatalf("round trip mismatch, got %+v", decoded.Data)
		}
	})

	t.Run("emptyData", func(t *testing.T) {
		origin := &Message[itPayload]{MsgId: "id-empty"}
		s := origin.Marshall()

		decoded := &Message[itPayload]{}
		if err := json.Unmarshal([]byte(s), decoded); err != nil {
			t.Fatalf("unmarshal %s failed: %s", s, err)
		}
		if decoded.Data != (itPayload{}) {
			t.Fatalf("expected zero data, got %+v", decoded.Data)
		}
	})
}

func TestMessage_TypeMismatchUnmarshalFails(t *testing.T) {
	s := (&Message[itPayload]{MsgId: "id-1", Data: itPayload{RoomId: "r", Count: 1}}).Marshall()

	decoded := &Message[int64]{}
	if err := json.Unmarshal([]byte(s), decoded); err == nil {
		t.Fatalf("unmarshalling an object payload into int64 should fail, got %+v", decoded)
	}
}

func TestEvent_JSONShape(t *testing.T) {
	e := &event{Id: "id-1", Key: "k-1", Args: `{"msgId":"m"}`, RetryCount: 2, UnAckRetryCount: 3}

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event failed: %s", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal event failed: %s", err)
	}

	want := map[string]any{
		"id":              "id-1",
		"key":             "k-1",
		"args":            `{"msgId":"m"}`,
		"retryCount":      float64(2),
		"unAckRetryCount": float64(3),
	}
	for k, v := range want {
		if raw[k] != v {
			t.Fatalf("event field %s expected %+v, got %+v", k, v, raw[k])
		}
	}
	if len(raw) != len(want) {
		t.Fatalf("event should serialize exactly %d fields, got %+v", len(want), raw)
	}

	decoded := new(event)
	if err := json.Unmarshal(b, decoded); err != nil {
		t.Fatalf("unmarshal event failed: %s", err)
	}
	if *decoded != *e {
		t.Fatalf("event round trip mismatch, got %+v", decoded)
	}
}

func TestEvent_ZeroValueIsPublishable(t *testing.T) {
	e := &event{Key: "k", Args: "a", Id: "i"}
	s := mustEventJSON(t, e)

	decoded := unmarshalEvent(t, s)
	if decoded.RetryCount != 0 || decoded.UnAckRetryCount != 0 {
		t.Fatalf("retry counters should default to 0, got retry: %d, unAckRetry: %d", decoded.RetryCount, decoded.UnAckRetryCount)
	}
}
