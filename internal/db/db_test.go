//go:build integration

package db

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestDB_DialogLastMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	database, err := New(ctx)
	if err != nil {
		t.Skipf("database is not configured: %v", err)
	}
	defer database.Close()

	const (
		dialogID = uint64(160)
		limit    = uint16(10)
	)

	got, err := database.DialogLastMessages(ctx, dialogID, limit)
	if err != nil {
		t.Fatalf("DialogLastMessages() error = %v", err)
	}
	if !json.Valid(got) {
		t.Fatalf("DialogLastMessages() returned invalid JSON: %s", string(got))
	}

	var msgs []json.RawMessage
	if err := json.Unmarshal(got, &msgs); err != nil {
		t.Fatalf("failed to unmarshal messages: %v; raw: %s", err, string(got))
	}
	if len(msgs) == 0 {
		t.Skipf("expected non-empty messages, got 0 (database may not have test data)")
	}
	if len(msgs) > int(limit) {
		t.Fatalf("expected len(msgs) <= %d, got %d", limit, len(msgs))
	}
	for i, m := range msgs {
		if !json.Valid(m) {
			t.Fatalf("message[%d] is not valid JSON: %s", i, string(m))
		}
	}
}
