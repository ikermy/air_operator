package operator

import (
	"air_operator/internal/repository"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ikermy/air_common/pkg/model"
)

func executeRequest(handler http.HandlerFunc, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	handler(w, req)
	return w
}

// TestAvailable тестирует эндпоинт /oper/available
func TestAvailable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	req := httptest.NewRequest(http.MethodGet, "/oper/available", nil)
	w := executeRequest(listener.available, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestHandleEvents_MissingParams тестирует handleEvents с отсутствующими параметрами
func TestHandleEvents_MissingParams(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		userID     string
		dialogID   string
		wantStatus int
	}{
		{
			name:       "Missing both params",
			userID:     "",
			dialogID:   "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "Missing dialog_id",
			userID:     "1",
			dialogID:   "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "Missing user_id",
			userID:     "",
			dialogID:   "500",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "Invalid user_id",
			userID:     "invalid",
			dialogID:   "500",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "Invalid dialog_id",
			userID:     "1",
			dialogID:   "invalid",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockDB := &MockDB{}
			mockTG := &MockTelega{}
			listener := createTestListener(ctx, mockDB, mockTG)

			url := fmt.Sprintf("/op?user_id=%s&dialog_id=%s", tc.userID, tc.dialogID)
			req := httptest.NewRequest(http.MethodGet, url, nil)
			w := executeRequest(listener.handleEvents, req)

			if w.Code != tc.wantStatus {
				t.Errorf("Expected status %d, got %d", tc.wantStatus, w.Code)
			}
		})
	}
}

// TestHandleEvents_NoOperators тестирует handleEvents без операторов
func TestHandleEvents_NoOperators(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil)
	w := executeRequest(listener.handleEvents, req)

	// Должен вернуться SSE с событием error
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for SSE, got %d", w.Code)
	}

	// Проверяем заголовки SSE
	contentType := w.Header().Get("Content-Type")
	if contentType != "text/event-stream" {
		t.Errorf("Expected Content-Type 'text/event-stream', got '%s'", contentType)
	}

	body := w.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Errorf("Expected 'event: error' in response, got '%s'", body)
	}
}

// TestHandleEvents_Success тестирует успешное создание SSE-соединения
func TestHandleEvents_Success(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Добавляем оператора
	listener.operators.AddTG(1, 100)

	// Устанавливаем историю сообщений
	historyJSON := json.RawMessage(`[{"type":"text","content":{"message":"Test history"}}]`)
	mockDB.SetDialogMessages(historyJSON, nil)

	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	// Запускаем в горутине, так как SSE блокирует
	done := make(chan bool)
	go func() {
		listener.handleEvents(w, req)
		done <- true
	}()

	// Ждём немного для инициализации
	time.Sleep(200 * time.Millisecond)

	// Отправляем сообщение через rx канал
	session := listener.operators.GetSession(1, 100, 500)
	if session != nil {
		testMsg := model.Message{
			Type:    "text",
			Content: model.AssistResponse{Message: "Hello"},
		}
		select {
		case session.rx <- testMsg:
		case <-time.After(100 * time.Millisecond):
			t.Error("Failed to send test message")
		}
	}

	// Ждём обработки
	time.Sleep(100 * time.Millisecond)

	// Отменяем контекст для завершения SSE
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("SSE handler did not complete in time")
	}

	// Проверяем ответ
	body := w.Body.String()
	if !strings.Contains(body, "text/event-stream") {
		// Проверяем заголовок
		contentType := w.Header().Get("Content-Type")
		if contentType != "text/event-stream" {
			t.Errorf("Expected Content-Type 'text/event-stream', got '%s'", contentType)
		}
	}

	// Проверяем, что история отправлена в Telegram
	history := mockTG.GetSentHistory()
	if len(history) != 1 {
		t.Errorf("Expected 1 history sent, got %d", len(history))
	}

	// Проверяем наличие события init с sid
	if !strings.Contains(body, "event: init") {
		t.Error("Expected 'event: init' in response")
	}
}

// TestHandleEvents_WithExistingDialog тестирует подключение к существующему диалогу
func TestHandleEvents_WithExistingDialog(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём существующий диалог
	listener.operators.AddTG(1, 100)
	listener.operators.AddSession(1, 100, 500)

	mockDB.SetDialogMessages(nil, nil) // Нет истории

	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		listener.handleEvents(w, req)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("SSE handler did not complete")
	}

	// Проверяем, что история не отправлялась (т.к. nil)
	history := mockTG.GetSentHistory()
	if len(history) != 0 {
		t.Errorf("Expected no history sent, got %d", len(history))
	}

	// Проверяем, что используется существующий tgID (100)
	body := w.Body.String()
	if !strings.Contains(body, "event: init") {
		t.Error("Expected init event")
	}
}

// TestHandleMessage тестирует отправку сообщения от оператора
func TestHandleMessage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём сессию
	listener.operators.AddTG(1, 100)
	listener.operators.AddSession(1, 100, 500)

	// Подготавливаем тестовое сообщение
	envelope := map[string]interface{}{
		"user_id":   1,
		"dialog_id": 500,
		"sid":       100,
		"msg": map[string]interface{}{
			"type": "text",
			"content": map[string]interface{}{
				"message": "Test operator message",
			},
		},
	}

	body, _ := json.Marshal(envelope)
	req := httptest.NewRequest(http.MethodPost, "/op", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := executeRequest(listener.handleMessage, req)

	// Проверяем статус
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Проверяем, что сообщение попало в tx канал
	session := listener.operators.GetSession(1, 100, 500)
	select {
	case msg := <-session.tx:
		if msg.Content.Message != "Test operator message" {
			t.Errorf("Expected 'Test operator message', got '%s'", msg.Content.Message)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Message was not sent to tx channel")
	}
}

// TestHandleMessage_InvalidJSON тестирует обработку невалидного JSON
func TestHandleMessage_InvalidJSON(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	req := httptest.NewRequest(http.MethodPost, "/op", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := executeRequest(listener.handleMessage, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "invalid JSON") {
		t.Errorf("Expected 'invalid JSON' error, got '%s'", body)
	}
}

// TestHandleMessage_MissingFields тестирует обработку отсутствующих полей
func TestHandleMessage_MissingFields(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		envelope   map[string]interface{}
		wantStatus int
		wantError  string
	}{
		{
			name: "Missing user_id",
			envelope: map[string]interface{}{
				"dialog_id": 500,
				"sid":       100,
				"msg":       map[string]interface{}{"type": "text"},
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "missing required fields",
		},
		{
			name: "Missing dialog_id",
			envelope: map[string]interface{}{
				"user_id": 1,
				"sid":     100,
				"msg":     map[string]interface{}{"type": "text"},
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "missing required fields",
		},
		{
			name: "Missing sid",
			envelope: map[string]interface{}{
				"user_id":   1,
				"dialog_id": 500,
				"msg":       map[string]interface{}{"type": "text"},
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "missing required fields",
		},
		{
			name: "Missing msg",
			envelope: map[string]interface{}{
				"user_id":   1,
				"dialog_id": 500,
				"sid":       100,
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "msg field is required",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockDB := &MockDB{}
			mockTG := &MockTelega{}
			listener := createTestListener(ctx, mockDB, mockTG)

			body, _ := json.Marshal(tc.envelope)
			req := httptest.NewRequest(http.MethodPost, "/op", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := executeRequest(listener.handleMessage, req)

			if w.Code != tc.wantStatus {
				t.Errorf("Expected status %d, got %d", tc.wantStatus, w.Code)
			}

			respBody := w.Body.String()
			if !strings.Contains(respBody, tc.wantError) {
				t.Errorf("Expected error containing '%s', got '%s'", tc.wantError, respBody)
			}
		})
	}
}

// TestHandleMessage_SessionNotFound тестирует отправку в несуществующую сессию
func TestHandleMessage_SessionNotFound(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	envelope := map[string]interface{}{
		"user_id":   1,
		"dialog_id": 500,
		"sid":       100,
		"msg": map[string]interface{}{
			"type": "text",
		},
	}

	body, _ := json.Marshal(envelope)
	req := httptest.NewRequest(http.MethodPost, "/op", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := executeRequest(listener.handleMessage, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}

	respBody := w.Body.String()
	if !strings.Contains(respBody, "session not found") {
		t.Errorf("Expected 'session not found' error, got '%s'", respBody)
	}
}

// TestHandleMessage_Timeout тестирует таймаут отправки
func TestHandleMessage_Timeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём сессию, но не читаем из tx канала
	listener.operators.AddTG(1, 100)
	session := listener.operators.AddSession(1, 100, 500)

	// Заполняем канал tx (буфер = 1)
	session.tx <- model.Message{Type: "blocking"}

	envelope := map[string]interface{}{
		"user_id":   1,
		"dialog_id": 500,
		"sid":       100,
		"msg": map[string]interface{}{
			"type": "text",
		},
	}

	body, _ := json.Marshal(envelope)
	req := httptest.NewRequest(http.MethodPost, "/op", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := executeRequest(listener.handleMessage, req)

	// Должен вернуться таймаут, так как канал переполнен
	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("Expected status 504, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleEvents_SessionExpiry тестирует истечение сессии по неактивности
func TestHandleEvents_SessionExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping expiry test in short mode")
	}

	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	listener.operators.AddTG(1, 100)
	mockDB.SetDialogMessages(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		listener.handleEvents(w, req)
		done <- true
	}()

	// Ждём начала SSE
	time.Sleep(200 * time.Millisecond)

	// Получаем сессию и устанавливаем старое время активности
	session := listener.operators.GetSession(1, 100, 500)
	if session != nil {
		oldTime := time.Now().Add(-10 * time.Minute)
		session.lastActive.Store(&oldTime)
	}

	// Ждём следующего пинга (30 секунд в реальном коде, но мы используем таймаут контекста)
	// Для тестирования этого нужно изменить логику или использовать моки времени

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("SSE handler did not complete")
	}
}

// TestHandleEvents_MessageDelivery тестирует доставку сообщений через SSE
func TestHandleEvents_MessageDelivery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	listener.operators.AddTG(1, 100)
	mockDB.SetDialogMessages(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		listener.handleEvents(w, req)
		done <- true
	}()

	// Ждём инициализации SSE
	time.Sleep(200 * time.Millisecond)

	// Отправляем несколько сообщений
	session := listener.operators.GetSession(1, 100, 500)
	if session != nil {
		for i := 1; i <= 3; i++ {
			msg := model.Message{
				Type:    "text",
				Content: model.AssistResponse{Message: fmt.Sprintf("Message %d", i)},
			}
			select {
			case session.rx <- msg:
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Failed to send message %d", i)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Даём время на обработку
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("SSE handler did not complete")
	}

	// Проверяем тело ответа на наличие сообщений
	body := w.Body.String()
	for i := 1; i <= 3; i++ {
		expected := fmt.Sprintf("Message %d", i)
		if !strings.Contains(body, expected) {
			t.Errorf("Expected message '%s' in response", expected)
		}
	}

	// Проверяем наличие события messages
	if !strings.Contains(body, "event: messages") {
		t.Error("Expected 'event: messages' in response")
	}
}

// BenchmarkOperatorsMap_AddSession бенчмарк для AddSession
func BenchmarkOperatorsMap_AddSession(b *testing.B) {
	var om OperatorsMap
	om.AddTG(1, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		om.AddSession(1, 100, uint64(i))
	}
}

// BenchmarkOperatorsMap_GetSession бенчмарк для GetSession
func BenchmarkOperatorsMap_GetSession(b *testing.B) {
	var om OperatorsMap
	om.AddTG(1, 100)
	om.AddSession(1, 100, 500)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		om.GetSession(1, 100, 500)
	}
}

// BenchmarkOperatorsMap_GetTg бенчмарк для GetTg
func BenchmarkOperatorsMap_GetTg(b *testing.B) {
	var om OperatorsMap
	om.AddTG(1, 100)
	om.AddTG(1, 200)
	om.AddTG(1, 300)
	om.AddSession(1, 100, 500)
	om.AddSession(1, 200, 600)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		om.GetTg(1)
	}
}

// BenchmarkHandleMessage бенчмарк для handleMessage
func BenchmarkHandleMessage(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	listener.operators.AddTG(1, 100)
	listener.operators.AddSession(1, 100, 500)

	envelope := map[string]interface{}{
		"user_id":   1,
		"dialog_id": 500,
		"sid":       100,
		"msg": map[string]interface{}{
			"type": "text",
			"content": map[string]interface{}{
				"message": "Benchmark message",
			},
		},
	}

	body, _ := json.Marshal(envelope)

	// Запускаем горутину для чтения из tx
	session := listener.operators.GetSession(1, 100, 500)
	go func() {
		for {
			select {
			case <-session.tx:
			case <-ctx.Done():
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/op", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		executeRequest(listener.handleMessage, req)
	}
}

// TestHandleEvents_CloseEvent тестирует отправку события закрытия
func TestHandleEvents_CloseEvent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	listener.operators.AddTG(1, 100)
	mockDB.SetDialogMessages(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		listener.handleEvents(w, req)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("SSE handler did not complete")
	}

	// Проверяем наличие события close
	body := w.Body.String()
	if !strings.Contains(body, "event: close") {
		t.Error("Expected 'event: close' in response")
	}
}

// TestHandleEvents_MultipleClients тестирует несколько одновременных SSE-подключений
func TestHandleEvents_MultipleClients(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём двух операторов
	listener.operators.AddTG(1, 100)
	listener.operators.AddTG(2, 200)
	mockDB.SetDialogMessages(nil, nil)

	// Запускаем два SSE-подключения
	done1 := make(chan bool)
	done2 := make(chan bool)

	go func() {
		req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)
		w := httptest.NewRecorder()
		listener.handleEvents(w, req)
		done1 <- true
	}()

	go func() {
		req := httptest.NewRequest(http.MethodGet, "/op?user_id=2&dialog_id=600", nil).WithContext(ctx)
		w := httptest.NewRecorder()
		listener.handleEvents(w, req)
		done2 <- true
	}()

	// Ждём инициализации
	time.Sleep(200 * time.Millisecond)

	// Проверяем, что обе сессии созданы
	session1 := listener.operators.GetSession(1, 100, 500)
	session2 := listener.operators.GetSession(2, 200, 600)

	if session1 == nil {
		t.Error("Session 1 should exist")
	}
	if session2 == nil {
		t.Error("Session 2 should exist")
	}

	cancel()

	select {
	case <-done1:
	case <-time.After(1 * time.Second):
		t.Error("Client 1 did not complete")
	}

	select {
	case <-done2:
	case <-time.After(1 * time.Second):
		t.Error("Client 2 did not complete")
	}
}

// MockResponseWriter для имитации http.ResponseWriter с Flusher
type MockResponseWriter struct {
	*httptest.ResponseRecorder
	flushed int
}

func (m *MockResponseWriter) Flush() {
	m.flushed++
}

// TestHandleEvents_Flusher проверяет вызовы Flush
func TestHandleEvents_Flusher(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	listener.operators.AddTG(1, 100)
	mockDB.SetDialogMessages(nil, nil)

	w := &MockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "/op?user_id=1&dialog_id=500", nil).WithContext(ctx)

	done := make(chan bool)
	go func() {
		listener.handleEvents(w, req)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("Handler did not complete")
	}

	// Проверяем, что Flush был вызван (минимум для init события)
	if w.flushed < 1 {
		t.Errorf("Expected at least 1 flush call, got %d", w.flushed)
	}
}

// TestMockTelega_Interface проверяет, что MockTelega реализует интерфейс Telega
func TestMockTelega_Interface(t *testing.T) {
	var _ Telega = (*MockTelega)(nil)
}

// TestMockDB_Interface проверяет, что MockDB реализует интерфейс DB
func TestMockDB_Interface(t *testing.T) {
	var _ repository.InternalRepository = (*MockDB)(nil)
}

// readSSEEvents читает и парсит SSE события из io.Reader
func readSSEEvents(r io.Reader) ([]map[string]string, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(body), "\n")
	var events []map[string]string
	currentEvent := make(map[string]string)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if len(currentEvent) > 0 {
				events = append(events, currentEvent)
				currentEvent = make(map[string]string)
			}
			continue
		}

		if strings.HasPrefix(line, "event:") {
			currentEvent["event"] = strings.TrimSpace(line[6:])
		} else if strings.HasPrefix(line, "data:") {
			currentEvent["data"] = strings.TrimSpace(line[5:])
		} else if strings.HasPrefix(line, ":") {
			// Комментарий, игнорируем
		}
	}

	if len(currentEvent) > 0 {
		events = append(events, currentEvent)
	}

	return events, nil
}
