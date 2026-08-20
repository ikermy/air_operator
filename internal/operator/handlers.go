package operator

import (
	"air_operator/internal/domain"
	"air_operator/internal/metrics"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

func (l *Listener) available(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (l *Listener) Available(w http.ResponseWriter, r *http.Request) { l.available(w, r) }

// handleEvents обслуживает SSE (GET) для операторских сессий
func (l *Listener) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	// Требуемые параметры идентификации сессии
	userIDStr := r.URL.Query().Get("user_id")
	dialogIDStr := r.URL.Query().Get("dialog_id")
	if userIDStr == "" || dialogIDStr == "" {
		metrics.ObserveSSESession("bad_request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	userID64, err := strconv.ParseUint(userIDStr, 10, 32)
	if err != nil {
		metrics.ObserveSSESession("bad_request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dialogID, err := strconv.ParseUint(dialogIDStr, 10, 64)
	if err != nil {
		metrics.ObserveSSESession("bad_request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	userID := uint32(userID64)

	// Получаю tg_id, для пользователя может быть несколько TG аккаунтов
	operatorsTg := l.operators.GetTGs(userID)

	// Если нет TG аккаунтов, выхожу с ошибкой
	if len(operatorsTg) == 0 {
		metrics.ObserveSSESession("rejected_no_operator")
		logger.Debug("operators not found")

		// Устанавливаем SSE-заголовки
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Отправляем событие с ошибкой
		w.Write([]byte("event: error\n"))
		// Пока всего один тип обрабатываемой ошибки
		//w.Write([]byte("data: {\"error\":\"no tg_id associated with user\"}\n\n"))

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		return
	}

	// Ищу активные диалоги
	dialogExist := false
	var tgID int64
	for _, tg := range operatorsTg {
		if l.operators.HasDialog(userID, tg, dialogID) {
			// Нашёл активный диалог, получаю его и выхожу из цикла
			dialogExist = true
			tgID = tg
			break
		}
	}

	// Если активный диалог не найден — назначаем оператора (самый свободный tgID)
	if !dialogExist {
		tgID = l.operators.GetTg(userID)
		// Если почему-то не удалось выбрать tgID
		if tgID == 0 {
			metrics.ObserveSSESession("rejected_no_tg")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// Всё больше не нужно отдавать ошибки, отдаю заголовки SSE
	// === GET: открыть SSE-соединение ===
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Создаём/заменяем канал исходящих сообщений для этой сессии в OperatorsMap
	session := l.operators.AddSession(userID, tgID, dialogID)
	metrics.ObserveSSESession("opened")

	// Получаю историю сообщений если она есть
	historyStartedAt := time.Now()
	data, err := l.db.DialogLastMessages(dialogID, domain.HistoryLimitMessages)
	if err != nil {
		metrics.ObserveHistoryRequest("error", historyStartedAt)
		logger.Error("failed to get last messages for dialog %d: %v", dialogID, err)
	} else if data == nil {
		// Если истории нет, пропускаю отправку
		metrics.ObserveHistoryRequest("empty", historyStartedAt)
	} else {
		metrics.ObserveHistoryRequest("success", historyStartedAt)
		//logger.Warn(string(data))

		// Отправляю историю сообщений
		sendStartedAt := time.Now()
		if l.tg != nil {
			err = l.tg.SendHistory(tgID, data)
			if err != nil {
				metrics.ObserveTelegramSend("history", "error", sendStartedAt)
				logger.Error("failed to send history to tg_id %d for dialog %d: %v", tgID, dialogID, err)
			} else {
				metrics.ObserveTelegramSend("history", "success", sendStartedAt)
			}
		} else {
			metrics.ObserveTelegramSend("history", "error", sendStartedAt)
			logger.Warn("telegram sender is not configured for history delivery")
		}
	}

	// Создаем контекст для жизни сессии
	sessionCtx, sessionCancel := context.WithCancel(r.Context())
	defer sessionCancel()

	// Запускаю промежуточный слой слушателя сообщений
	go l.operatorLayerListener(sessionCtx, session, userID, tgID, dialogID)

	// Пинг каждые 30с
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()

	logger.Info("SSE connected: user=%d tg=%d dialog=%d", userID, tgID, dialogID)

	defer func() {
		// Сначала пытаемся уведомить клиента о завершении SSE-сессии
		if _, err := fmt.Fprintf(w, "event: close\n"); err == nil {
			if _, err := fmt.Fprintf(w, "data: {\"type\":\"closed\",\"message\":\"SSE session terminated\"}\n\n"); err == nil {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}

		// Удаляем сессию только если ещё наш канал актуален (защита от гонок при переподключении)
		_ = l.operators.RemoveSession(userID, tgID, dialogID, session)

		// Сбрасываем маршрут, если он указывает на закрытую сессию
		if uid, did, ok := l.getLastRoute(tgID); ok && uid == userID && did == dialogID {
			l.lastRoute.Delete(tgID)
		}

		metrics.ObserveSSESession("closed")
		logger.Info("SSE disconnected: user=%d tg=%d dialog=%d", userID, tgID, dialogID)
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Немедленный комментарий, чтобы зафиксировать соединение
	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
		return
	}
	// Отправляем назначенный tgID клиенту
	initData := map[string]interface{}{
		//"type":      "init",
		"sid": tgID,
	}
	initJSON, _ := json.Marshal(initData)
	if _, err := fmt.Fprintf(w, "event: init\n"); err != nil {
		return
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", initJSON); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			// Клиент отключился
			logger.Debug("SSE context done: tg=%d dialog=%d err=%v", tgID, dialogID, r.Context().Err(), userID)
			return

		case <-ping.C:

			// Проверяем активность сессии
			if session.IsExpired(5 * time.Minute) {
				logger.Debug("Сессия неактивна, закрываем: user=%d tg=%d dialog=%d", userID, tgID, dialogID)
				metrics.ObserveSSESession("timeout")

				// Отправляем событие о закрытии по неактивности
				if _, err := fmt.Fprintf(w, "event: timeout\n"); err == nil {
					if _, err := fmt.Fprintf(w, "data: {\"type\":\"inactive\",\"message\":\"Session closed due to inactivity\"}\n\n"); err == nil {
						flusher.Flush()
					}
				}
				return
			}

			// Периодический ping-комментарий
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()

		case msg, ok := <-session.rx:
			if !ok {
				// Канал закрыт — завершаем
				logger.Debug("SSE rx closed: tg=%d dialog=%d", tgID, dialogID, userID)
				return
			}
			// Отправляем событие с именем "messages" и данными msg в JSON
			payload, err := json.Marshal(msg)
			if err != nil {
				logger.Warn("serialize SSE message failed: %v", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "event: messages\n"); err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (l *Listener) Events(w http.ResponseWriter, r *http.Request) { l.handleEvents(w, r) }

// handleMessage принимает сообщение от клиента и перенаправляет его в соответствующую SSE-сессию
func (l *Listener) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	type envelope struct {
		UserID   uint32         `json:"user_id"`
		DialogID uint64         `json:"dialog_id"`
		TgID     int64          `json:"sid"` // бывший tg_id Telegram ID оператора
		Msg      *model.Message `json:"msg,omitempty"`
	}
	var env envelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		metrics.ObserveMessageEvent("widget", "invalid_json", "unknown")
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if env.UserID == 0 || env.DialogID == 0 || env.TgID == 0 {
		metrics.ObserveMessageEvent("widget", "invalid", messageType(env.Msg))
		http.Error(w, "missing required fields: user_id, dialog_id, tg_id", http.StatusBadRequest)
		return
	}

	if env.Msg == nil {
		metrics.ObserveMessageEvent("widget", "missing_message", "unknown")
		http.Error(w, "msg field is required", http.StatusBadRequest)
		return
	}
	metrics.ObserveMessageEvent("widget", "received", env.Msg.Type)

	// Ищем активную сессию в OperatorsMap
	session := l.operators.GetSession(env.UserID, env.TgID, env.DialogID)
	if session == nil {
		metrics.ObserveMessageEvent("widget", "session_not_found", env.Msg.Type)
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	// Обновляю время последней активности оператора
	l.operators.UpdateLastActive(env.UserID, env.TgID, env.DialogID)

	select {
	case session.tx <- *env.Msg:
		metrics.ObserveMessageEvent("widget", "queued", env.Msg.Type)
		w.WriteHeader(http.StatusOK)
	case <-time.After(3 * time.Second):
		metrics.ObserveMessageEvent("widget", "timeout", env.Msg.Type)
		http.Error(w, "send timeout", http.StatusGatewayTimeout)
	}
}

func (l *Listener) Message(w http.ResponseWriter, r *http.Request) { l.handleMessage(w, r) }

func messageType(msg *model.Message) string {
	if msg == nil {
		return "unknown"
	}
	return msg.Type
}
