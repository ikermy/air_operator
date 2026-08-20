package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func GetConf() *conf.Conf {
	// Переходим в корневую директорию проекта
	if err := os.Chdir("../../.."); err != nil {
		log.Fatalf("Failed to change to root directory: %v", err)
	}

	cfg, err := conf.NewConf()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	return cfg
}

func TestDB_DialogLastMessages(t *testing.T) {
	t.Parallel()

	cfg := GetConf()
	ctx := context.Background()

	db := New(ctx, cfg)
	// Явно закрываем ресурсы, не используем HandlerClose, чтобы не зависеть от глобальных каналов.
	defer func() {
		db.cancel()
		_ = db.conn.Close()
	}()

	dialogID := uint64(160)
	limit := uint8(10)

	got, err := db.DialogLastMessages(dialogID, limit)
	if err != nil {
		t.Fatalf("DialogLastMessages() error = %v", err)
	}

	// Проверяем, что это валидный JSON-массив
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

	// Дополнительно проверим, что каждый элемент валиден как JSON
	for i, m := range msgs {
		if !json.Valid(m) {
			t.Fatalf("message[%d] is not valid JSON: %s", i, string(m))
		}
	}

	t.Logf("DialogLastMessages() OK: dialogID=%d, count=%d (limit=%d)\nfirst=%s",
		dialogID, len(msgs), limit, truncate(string(msgs[0]), 200))

	for _, m := range msgs {
		fmt.Println(string(m))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return fmt.Sprintf("%s...(%d bytes)", s[:n], len(s))
}
