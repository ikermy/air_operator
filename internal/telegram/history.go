package telegram

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	tele "gopkg.in/telebot.v4"
)

// CreatorType и константы автора
type CreatorType uint8

const (
	AI        CreatorType = 1 // Право
	User      CreatorType = 2 // Лево
	UserVoice CreatorType = 3 // Лево
	Operator  CreatorType = 4 // Прав
)

// Структура одного сообщения из БД под реальный JSON
// {"responder_name":"...","message":{"creator":2,"message":{"message":"...","action":{}},"timestamp":"..."}}
type dbHistMsg struct {
	Responder string `json:"responder_name"`
	Envelope  struct {
		Creator CreatorType `json:"creator"`
		Message struct {
			Message string `json:"message"`
			// Action можно добавить при необходимости
		} `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"message"`
}

// Отправляет историю сообщений в чат Telegram.
// raw должен быть JSON-массивом сообщений или потоком объектов.
func (t *Telega) SendHistory(recipient int64, raw []byte) error {
	if t.b == nil {
		return nil
	}

	data := bytes.TrimSpace(raw)
	if len(data) == 0 {
		return nil
	}

	var msgs []dbHistMsg
	if data[0] == '[' {
		if err := json.Unmarshal(data, &msgs); err != nil {
			return err
		}
	} else {
		dec := json.NewDecoder(bytes.NewReader(data))
		for {
			var m dbHistMsg
			if err := dec.Decode(&m); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			msgs = append(msgs, m)
		}
	}

	// Фильтруем пустые и без времени
	filtered := make([]dbHistMsg, 0, len(msgs))
	for _, m := range msgs {
		if strings.TrimSpace(m.Envelope.Message.Message) == "" || m.Envelope.Timestamp.IsZero() {
			continue
		}
		filtered = append(filtered, m)
	}

	// Форматирование с нормализацией переводов строк
	var b strings.Builder
	for _, m := range filtered {
		ts := m.Envelope.Timestamp.Local().Format("02.01 15:04")
		b.WriteString("[")
		b.WriteString(ts)
		b.WriteString("] ")
		b.WriteString(authorOf(m.Envelope.Creator, m.Responder))
		b.WriteString(": ")
		b.WriteString(cleanMsgText(m.Envelope.Message.Message))
		b.WriteString("\n")
	}

	text := strings.TrimSpace(b.String())
	if text == "" {
		return nil
	}

	const tgLimit = 4096
	for _, part := range splitTelegram(text, tgLimit) {
		if _, err := t.sendWithRetry(recipient, part, &tele.SendOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func authorOf(c CreatorType, responder string) string {
	switch c {
	case AI:
		return "AI"
	case User, UserVoice:
		name := strings.TrimSpace(responder)
		if name != "" {
			return name
		}
		if c == UserVoice {
			return "User(voice)"
		}
		return "User"
	case Operator:
		return "Operator"
	default:
		return "Unknown"
	}
}

// Удаляет Windows-CRLF и хвостовые переводы строки, чтобы не было двойных \n
func cleanMsgText(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.TrimRight(s, "\n\r")
	return s
}

func splitTelegram(text string, limit int) []string {
	if len(text) <= limit {
		return []string{text}
	}
	var parts []string
	for len(text) > 0 {
		if len(text) <= limit {
			parts = append(parts, text)
			break
		}
		cut := strings.LastIndex(text[:limit], "\n")
		if cut <= 0 {
			cut = limit
		}
		parts = append(parts, text[:cut])
		text = text[cut:]
		// Убираем ведущий перевод строки у остатка
		if len(text) > 0 && text[0] == '\n' {
			text = text[1:]
		}
	}
	return parts
}
