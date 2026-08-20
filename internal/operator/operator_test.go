package operator

import (
	"air_operator/internal/domain"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ikermy/air_common/pkg/model"
)

// MockDB реализует интерфейс DB для тестирования
type MockDB struct {
	mu                     sync.Mutex
	operators              []domain.OperatorChannels
	operatorsErr           error
	dialogMessages         json.RawMessage
	dialogMessagesErr      error
	filterChangedLastValue bool
}

func (m *MockDB) GetOperators(_ context.Context, filterChanged bool) ([]domain.OperatorChannels, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.filterChangedLastValue = filterChanged
	return m.operators, m.operatorsErr
}

func (m *MockDB) DialogLastMessages(_ context.Context, _ uint64, _ uint16) (json.RawMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dialogMessages, m.dialogMessagesErr
}

func (m *MockDB) SetOperators(ops []domain.OperatorChannels) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operators = ops
}

func (m *MockDB) SetOperatorsError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operatorsErr = err
}

func (m *MockDB) SetDialogMessages(msg json.RawMessage, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dialogMessages = msg
	m.dialogMessagesErr = err
}

// MockTelega реализует интерфейс Telega для тестирования
type MockTelega struct {
	mu              sync.Mutex
	sentMessages    []SentMessage
	sentHistory     []SentHistory
	sendMsgErr      error
	sendHistoryErr  error
	incomingHandler func(int64, model.Message)
	closeSSEHandler func(int64)
}

type SentMessage struct {
	Recipient int64
	Message   model.Message
	MessageID int
}

type SentHistory struct {
	Recipient int64
	Raw       []byte
}

func (m *MockTelega) SendMsg(recipient int64, message model.Message) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendMsgErr != nil {
		return 0, m.sendMsgErr
	}
	msgID := len(m.sentMessages) + 1
	m.sentMessages = append(m.sentMessages, SentMessage{
		Recipient: recipient,
		Message:   message,
		MessageID: msgID,
	})
	return msgID, nil
}

func (m *MockTelega) SendHistory(recipient int64, raw []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendHistoryErr != nil {
		return m.sendHistoryErr
	}
	m.sentHistory = append(m.sentHistory, SentHistory{
		Recipient: recipient,
		Raw:       raw,
	})
	return nil
}

func (m *MockTelega) SetIncomingHandler(handler func(int64, model.Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incomingHandler = handler
}

func (m *MockTelega) SetCloseSSE(handler func(int64)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeSSEHandler = handler
}

func (m *MockTelega) GetSentMessages() []SentMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]SentMessage, len(m.sentMessages))
	copy(result, m.sentMessages)
	return result
}

func (m *MockTelega) GetSentHistory() []SentHistory {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]SentHistory, len(m.sentHistory))
	copy(result, m.sentHistory)
	return result
}

func (m *MockTelega) ClearSentMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = nil
	m.sentHistory = nil
}

func (m *MockTelega) SetSendMsgError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendMsgErr = err
}

func (m *MockTelega) SetSendHistoryError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendHistoryErr = err
}

// createTestListener создаёт Listener с mock-зависимостями для тестирования
func createTestListener(ctx context.Context, mockDB *MockDB, mockTG *MockTelega) *Listener {
	return &Listener{
		ctx:            ctx,
		cancel:         func() {},
		db:             mockDB,
		tg:             mockTG,
		carpinteroName: "test_bot",
	}
}

// TestOperatorsMap_AddTG тестирует добавление Telegram ID
func TestOperatorsMap_AddTG(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Добавляем первый TG ID
	om.AddTG(1, 100)
	tgs := om.GetTGs(1)
	if len(tgs) != 1 || tgs[0] != 100 {
		t.Errorf("Expected [100], got %v", tgs)
	}

	// Добавляем второй TG ID для того же пользователя
	om.AddTG(1, 200)
	tgs = om.GetTGs(1)
	if len(tgs) != 2 {
		t.Errorf("Expected 2 TG IDs, got %d", len(tgs))
	}

	// Проверяем, что оба ID присутствуют
	found100, found200 := false, false
	for _, tg := range tgs {
		if tg == 100 {
			found100 = true
		}
		if tg == 200 {
			found200 = true
		}
	}
	if !found100 || !found200 {
		t.Errorf("Expected both 100 and 200, got %v", tgs)
	}
}

// TestOperatorsMap_AddTG_Concurrent проверяет потокобезопасность AddTG
func TestOperatorsMap_AddTG_Concurrent(t *testing.T) {
	t.Parallel()

	var om OperatorsMap
	var wg sync.WaitGroup

	// Запускаем 10 горутин, каждая добавляет TG ID
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			om.AddTG(1, id)
		}(int64(100 + i))
	}

	wg.Wait()

	tgs := om.GetTGs(1)
	if len(tgs) != 10 {
		t.Errorf("Expected 10 TG IDs after concurrent adds, got %d", len(tgs))
	}
}

// TestOperatorsMap_AddSession тестирует создание сессий
func TestOperatorsMap_AddSession(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Добавляем TG и сессию
	om.AddTG(1, 100)
	session := om.AddSession(1, 100, 500)

	if session == nil {
		t.Fatal("Expected non-nil session")
	}

	if session.tx == nil || session.rx == nil {
		t.Error("Expected tx and rx channels to be initialized")
	}

	// Проверяем, что сессия доступна через GetSession
	retrieved := om.GetSession(1, 100, 500)
	if retrieved != session {
		t.Error("GetSession returned different session")
	}
}

// TestOperatorsMap_AddSession_Replacement тестирует замену существующей сессии
func TestOperatorsMap_AddSession_Replacement(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	oldSession := om.AddSession(1, 100, 500)

	// Проверяем, что старая сессия работает
	oldTx := oldSession.tx
	oldRx := oldSession.rx

	// Добавляем новую сессию с теми же параметрами
	newSession := om.AddSession(1, 100, 500)

	if newSession == oldSession {
		t.Error("Expected new session to be different from old")
	}

	// Проверяем, что старые каналы закрыты
	select {
	case _, ok := <-oldTx:
		if ok {
			t.Error("Old tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Old tx channel should be closed immediately")
	}

	select {
	case _, ok := <-oldRx:
		if ok {
			t.Error("Old rx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Old rx channel should be closed immediately")
	}
}

// TestOperatorsMap_GetSession тестирует получение сессии
func TestOperatorsMap_GetSession(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Попытка получить несуществующую сессию
	session := om.GetSession(1, 100, 500)
	if session != nil {
		t.Error("Expected nil for non-existent session")
	}

	// Создаём сессию и получаем её
	om.AddTG(1, 100)
	created := om.AddSession(1, 100, 500)
	retrieved := om.GetSession(1, 100, 500)

	if retrieved != created {
		t.Error("Retrieved session does not match created session")
	}
}

// TestOperatorsMap_RemoveSession тестирует удаление сессии
func TestOperatorsMap_RemoveSession(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	session := om.AddSession(1, 100, 500)

	// Удаляем сессию
	removed := om.RemoveSession(1, 100, 500, session)
	if !removed {
		t.Error("Expected RemoveSession to return true")
	}

	// Проверяем, что сессия удалена
	retrieved := om.GetSession(1, 100, 500)
	if retrieved != nil {
		t.Error("Session should be removed")
	}

	// Проверяем, что каналы закрыты
	select {
	case _, ok := <-session.tx:
		if ok {
			t.Error("tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("tx channel should be closed immediately")
	}
}

// TestOperatorsMap_RemoveSession_WrongSession проверяет, что не удаляется другая сессия
func TestOperatorsMap_RemoveSession_WrongSession(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	session1 := om.AddSession(1, 100, 500)
	session2 := om.AddSession(1, 100, 500) // Заменяет session1

	// Попытка удалить старую сессию
	removed := om.RemoveSession(1, 100, 500, session1)
	if removed {
		t.Error("Should not remove old session reference")
	}

	// Проверяем, что текущая сессия всё ещё существует
	current := om.GetSession(1, 100, 500)
	if current != session2 {
		t.Error("Current session should still be session2")
	}
}

// TestOperatorsMap_RemoveDialog тестирует удаление диалога
func TestOperatorsMap_RemoveDialog(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	session := om.AddSession(1, 100, 500)

	// Удаляем диалог
	om.RemoveDialog(1, 100, 500)

	// Проверяем, что диалог удалён
	retrieved := om.GetSession(1, 100, 500)
	if retrieved != nil {
		t.Error("Dialog should be removed")
	}

	// Проверяем, что каналы закрыты
	select {
	case _, ok := <-session.tx:
		if ok {
			t.Error("tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("tx channel should be closed immediately")
	}
}

// TestOperatorsMap_RemoveTG тестирует удаление Telegram ID со всеми диалогами
func TestOperatorsMap_RemoveTG(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	session1 := om.AddSession(1, 100, 500)
	session2 := om.AddSession(1, 100, 600)

	// Удаляем TG ID
	om.RemoveTG(1, 100)

	// Проверяем, что все диалоги удалены
	if om.GetSession(1, 100, 500) != nil {
		t.Error("Dialog 500 should be removed")
	}
	if om.GetSession(1, 100, 600) != nil {
		t.Error("Dialog 600 should be removed")
	}

	// Проверяем, что каналы закрыты
	select {
	case _, ok := <-session1.tx:
		if ok {
			t.Error("session1 tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("session1 tx channel should be closed immediately")
	}

	select {
	case _, ok := <-session2.tx:
		if ok {
			t.Error("session2 tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("session2 tx channel should be closed immediately")
	}

	// Проверяем, что TG ID больше не существует
	tgs := om.GetTGs(1)
	if len(tgs) != 0 {
		t.Errorf("Expected no TG IDs, got %d", len(tgs))
	}
}

// TestOperatorsMap_DeleteUser тестирует удаление пользователя
func TestOperatorsMap_DeleteUser(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	om.AddTG(1, 200)
	session1 := om.AddSession(1, 100, 500)
	session2 := om.AddSession(1, 200, 600)

	// Удаляем пользователя
	om.DeleteUser(1)

	// Проверяем, что все данные удалены
	tgs := om.GetTGs(1)
	if len(tgs) != 0 {
		t.Errorf("Expected no TG IDs after DeleteUser, got %d", len(tgs))
	}

	// Проверяем, что каналы закрыты
	select {
	case _, ok := <-session1.tx:
		if ok {
			t.Error("session1 tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("session1 tx channel should be closed immediately")
	}

	select {
	case _, ok := <-session2.tx:
		if ok {
			t.Error("session2 tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("session2 tx channel should be closed immediately")
	}
}

// TestOperatorsMap_GetTGs тестирует получение списка Telegram IDs
func TestOperatorsMap_GetTGs(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Пустой список для несуществующего пользователя
	tgs := om.GetTGs(999)
	if tgs != nil {
		t.Error("Expected nil for non-existent user")
	}

	// Добавляем TG IDs
	om.AddTG(1, 100)
	om.AddTG(1, 200)
	om.AddTG(1, 300)

	tgs = om.GetTGs(1)
	if len(tgs) != 3 {
		t.Errorf("Expected 3 TG IDs, got %d", len(tgs))
	}

	// Проверяем наличие всех ID
	idMap := make(map[int64]bool)
	for _, id := range tgs {
		idMap[id] = true
	}

	if !idMap[100] || !idMap[200] || !idMap[300] {
		t.Errorf("Expected [100, 200, 300], got %v", tgs)
	}
}

// TestOperatorsMap_GetTg тестирует выбор наименее загруженного TG ID
func TestOperatorsMap_GetTg(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Нет пользователя
	tg := om.GetTg(999)
	if tg != 0 {
		t.Errorf("Expected 0 for non-existent user, got %d", tg)
	}

	// Один TG без диалогов
	om.AddTG(1, 100)
	tg = om.GetTg(1)
	if tg != 100 {
		t.Errorf("Expected 100, got %d", tg)
	}

	// Два TG: один пустой, один с диалогом - должен вернуть пустой
	om.AddTG(1, 200)
	om.AddSession(1, 100, 500)
	tg = om.GetTg(1)
	if tg != 200 {
		t.Errorf("Expected 200 (idle TG), got %d", tg)
	}

	// Оба TG с одинаковым числом диалогов - вернёт любой
	om.AddSession(1, 200, 600)
	tg = om.GetTg(1)
	if tg != 100 && tg != 200 {
		t.Errorf("Expected 100 or 200, got %d", tg)
	}

	// TG 100 с меньшим числом диалогов
	om.AddSession(1, 200, 700)
	tg = om.GetTg(1)
	if tg != 100 {
		t.Errorf("Expected 100 (fewer dialogs), got %d", tg)
	}
}

// TestOperatorsMap_GetDialogs тестирует получение списка диалогов
func TestOperatorsMap_GetDialogs(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Несуществующий пользователь/TG
	dialogs := om.GetDialogs(999, 100)
	if dialogs != nil {
		t.Error("Expected nil for non-existent user/tg")
	}

	// Добавляем диалоги
	om.AddTG(1, 100)
	om.AddSession(1, 100, 500)
	om.AddSession(1, 100, 600)
	om.AddSession(1, 100, 700)

	dialogs = om.GetDialogs(1, 100)
	if len(dialogs) != 3 {
		t.Errorf("Expected 3 dialogs, got %d", len(dialogs))
	}

	// Проверяем наличие всех диалогов
	dlgMap := make(map[uint64]bool)
	for _, id := range dialogs {
		dlgMap[id] = true
	}

	if !dlgMap[500] || !dlgMap[600] || !dlgMap[700] {
		t.Errorf("Expected [500, 600, 700], got %v", dialogs)
	}
}

// TestOperatorsMap_HasDialog тестирует проверку наличия диалога
func TestOperatorsMap_HasDialog(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	// Несуществующий диалог
	if om.HasDialog(1, 100, 500) {
		t.Error("Expected false for non-existent dialog")
	}

	// Добавляем диалог
	om.AddTG(1, 100)
	om.AddSession(1, 100, 500)

	if !om.HasDialog(1, 100, 500) {
		t.Error("Expected true for existing dialog")
	}

	// Проверяем другой диалог
	if om.HasDialog(1, 100, 999) {
		t.Error("Expected false for non-existent dialog 999")
	}
}

// TestOperatorsMap_UpdateLastActive тестирует обновление времени активности
func TestOperatorsMap_UpdateLastActive(t *testing.T) {
	t.Parallel()

	var om OperatorsMap

	om.AddTG(1, 100)
	session := om.AddSession(1, 100, 500)

	// Получаем начальное время
	initialTime := session.GetLastActive()

	// Ждём немного
	time.Sleep(10 * time.Millisecond)

	// Обновляем время активности
	om.UpdateLastActive(1, 100, 500)

	// Проверяем, что время обновилось
	newTime := session.GetLastActive()
	if !newTime.After(initialTime) {
		t.Error("LastActive should be updated to a later time")
	}
}

// TestSessionInfo_IsExpired тестирует проверку истечения срока сессии
func TestSessionInfo_IsExpired(t *testing.T) {
	t.Parallel()

	session := &SessionInfo{
		tx: make(chan model.Message, 1),
		rx: make(chan model.Message, 1),
	}

	// Устанавливаем время активности
	session.updateLastActive()

	// Сразу после обновления не должна быть истекшей
	if session.IsExpired(1 * time.Second) {
		t.Error("Session should not be expired immediately after update")
	}

	// Ждём и проверяем истечение
	time.Sleep(100 * time.Millisecond)
	if session.IsExpired(50 * time.Millisecond) {
		// Ожидаемо истекла
	} else {
		t.Error("Session should be expired after timeout")
	}

	// С большим таймаутом не должна истечь
	if session.IsExpired(10 * time.Second) {
		t.Error("Session should not be expired with large timeout")
	}
}

// TestDeliverIncomingFromTG тестирует доставку входящих сообщений от Telegram
func TestDeliverIncomingFromTG(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём сессию
	listener.operators.AddTG(1, 100)
	session := listener.operators.AddSession(1, 100, 500)

	// Устанавливаем маршрут
	listener.setLastRoute(100, 1, 500)

	// Создаём тестовое сообщение
	msg := model.Message{
		Type:    "text",
		Content: model.AssistResponse{Message: "Test message"},
		Name:    "User",
	}

	// Доставляем сообщение
	listener.DeliverIncomingFromTG(100, msg)

	// Проверяем, что сообщение попало в канал rx
	select {
	case received := <-session.rx:
		if received.Content.Message != "Test message" {
			t.Errorf("Expected 'Test message', got '%s'", received.Content.Message)
		}
		if !received.Operator.Operator || !received.Operator.SetOperator {
			t.Error("Message should be marked as operator message")
		}
	case <-time.After(1 * time.Second):
		t.Error("Message was not delivered to rx channel")
	}
}

// TestDeliverIncomingFromTG_NoRoute тестирует доставку без установленного маршрута
func TestDeliverIncomingFromTG_NoRoute(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	msg := model.Message{
		Type:    "text",
		Content: model.AssistResponse{Message: "Test"},
	}

	// Вызываем без установленного маршрута - не должно быть паники
	listener.DeliverIncomingFromTG(100, msg)

	// Тест проходит, если не было паники
}

// TestCloseSSE тестирует закрытие SSE-сессии
func TestCloseSSE(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём единственную сессию
	listener.operators.AddTG(1, 100)
	session := listener.operators.AddSession(1, 100, 500)
	listener.setLastRoute(100, 1, 500)

	// Закрываем SSE
	listener.CloseSSE(100)

	// Проверяем, что сессия удалена
	retrieved := listener.operators.GetSession(1, 100, 500)
	if retrieved != nil {
		t.Error("Session should be closed")
	}

	// Проверяем, что канал закрыт
	select {
	case _, ok := <-session.tx:
		if ok {
			t.Error("tx channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("tx channel should be closed immediately")
	}

	// Проверяем, что маршрут удалён
	_, _, ok := listener.getLastRoute(100)
	if ok {
		t.Error("Route should be deleted")
	}
}

// TestCloseSSE_MultipleDialogs тестирует, что SSE не закрывается при множественных диалогах
func TestCloseSSE_MultipleDialogs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём два диалога
	listener.operators.AddTG(1, 100)
	listener.operators.AddSession(1, 100, 500)
	listener.operators.AddSession(1, 100, 600)
	listener.setLastRoute(100, 1, 500)

	// Пытаемся закрыть SSE
	listener.CloseSSE(100)

	// Проверяем, что оба диалога всё ещё существуют
	session1 := listener.operators.GetSession(1, 100, 500)
	session2 := listener.operators.GetSession(1, 100, 600)

	if session1 == nil || session2 == nil {
		t.Error("Sessions should not be closed when there are multiple dialogs")
	}
}

// TestOperatorLayerListener тестирует слушателя сообщений оператора
func TestOperatorLayerListener(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Создаём сессию
	listener.operators.AddTG(1, 100)
	session := listener.operators.AddSession(1, 100, 500)

	// Запускаем слушателя в горутине
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	go listener.operatorLayerListener(sessionCtx, session, 1, 100, 500)

	// Отправляем сообщение в tx канал
	testMsg := model.Message{
		Type:    "text",
		Content: model.AssistResponse{Message: "Hello from operator"},
		Name:    "Operator",
	}

	session.tx <- testMsg

	// Ждём отправки
	time.Sleep(100 * time.Millisecond)

	// Проверяем, что сообщение отправлено в Telegram
	sent := mockTG.GetSentMessages()
	if len(sent) != 1 {
		t.Fatalf("Expected 1 sent message, got %d", len(sent))
	}

	if sent[0].Recipient != 100 {
		t.Errorf("Expected recipient 100, got %d", sent[0].Recipient)
	}

	if sent[0].Message.Content.Message != "Hello from operator" {
		t.Errorf("Expected 'Hello from operator', got '%s'", sent[0].Message.Content.Message)
	}

	// Проверяем, что маршрут установлен
	userID, dialogID, ok := listener.getLastRoute(100)
	if !ok || userID != 1 || dialogID != 500 {
		t.Error("Route should be set after successful send")
	}

	// Останавливаем слушателя
	sessionCancel()
	time.Sleep(50 * time.Millisecond)
}

// TestGetOperators_AddTelegramOperators тестирует добавление операторов Telegram
func TestGetOperators_AddTelegramOperators(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Устанавливаем тестовые данные операторов
	mockDB.SetOperators([]domain.OperatorChannels{
		{
			UserId:          1,
			Telegram:        []int64{100, 200},
			TelegramEnabled: true,
		},
		{
			UserId:          2,
			Telegram:        []int64{300},
			TelegramEnabled: true,
		},
	})

	// Вызываем GetOperators
	err := listener.GetOperators(false)
	if err != nil {
		t.Fatalf("GetOperators failed: %v", err)
	}

	// Проверяем, что операторы добавлены
	tgs1 := listener.operators.GetTGs(1)
	if len(tgs1) != 2 {
		t.Errorf("Expected 2 TG IDs for user 1, got %d", len(tgs1))
	}

	tgs2 := listener.operators.GetTGs(2)
	if len(tgs2) != 1 {
		t.Errorf("Expected 1 TG ID for user 2, got %d", len(tgs2))
	}
}

// TestGetOperators_DisableTelegramOperators тестирует отключение операторов
func TestGetOperators_DisableTelegramOperators(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Добавляем оператора с активным диалогом
	listener.operators.AddTG(1, 100)
	session := listener.operators.AddSession(1, 100, 500)
	listener.setLastRoute(100, 1, 500)

	// Отключаем Telegram оператора
	mockDB.SetOperators([]domain.OperatorChannels{
		{
			UserId:          1,
			Telegram:        []int64{100},
			TelegramEnabled: false,
		},
	})

	// Вызываем GetOperators
	err := listener.GetOperators(true)
	if err != nil {
		t.Fatalf("GetOperators failed: %v", err)
	}

	// Проверяем, что команда отправлена в rx
	select {
	case msg := <-session.rx:
		if msg.Type != "command" {
			t.Errorf("Expected command type, got %s", msg.Type)
		}
		if msg.Content.Message != "Set-Mode-To-AI" {
			t.Errorf("Expected 'Set-Mode-To-AI', got '%s'", msg.Content.Message)
		}
	case <-time.After(100 * time.Millisecond):
		// Канал может быть уже закрыт, это нормально
	}

	// Проверяем, что TG удалён
	tgs := listener.operators.GetTGs(1)
	if len(tgs) != 0 {
		t.Errorf("Expected no TG IDs after disable, got %d", len(tgs))
	}

	// Проверяем, что маршрут удалён
	_, _, ok := listener.getLastRoute(100)
	if ok {
		t.Error("Route should be deleted after disable")
	}
}

// TestGetOperators_EmptyData проверяет обработку пустых данных
func TestGetOperators_EmptyData(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Устанавливаем пустые данные
	mockDB.SetOperators([]domain.OperatorChannels{})

	// Вызываем GetOperators
	err := listener.GetOperators(false)
	if err != nil {
		t.Fatalf("GetOperators failed with empty data: %v", err)
	}

	// Проверяем, что ничего не добавлено
	tgs := listener.operators.GetTGs(1)
	if len(tgs) != 0 {
		t.Error("No operators should be added with empty data")
	}
}

// TestSetLastRoute_GetLastRoute тестирует установку и получение маршрута
func TestSetLastRoute_GetLastRoute(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	// Устанавливаем маршрут
	listener.setLastRoute(100, 1, 500)

	// Получаем маршрут
	userID, dialogID, ok := listener.getLastRoute(100)
	if !ok {
		t.Error("Route should exist")
	}

	if userID != 1 {
		t.Errorf("Expected userID 1, got %d", userID)
	}

	if dialogID != 500 {
		t.Errorf("Expected dialogID 500, got %d", dialogID)
	}

	// Проверяем несуществующий маршрут
	_, _, ok = listener.getLastRoute(999)
	if ok {
		t.Error("Route 999 should not exist")
	}
}

// TestSetLastRoute_Concurrent проверяет потокобезопасность маршрутов
func TestSetLastRoute_Concurrent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB := &MockDB{}
	mockTG := &MockTelega{}
	listener := createTestListener(ctx, mockDB, mockTG)

	var wg sync.WaitGroup

	// Запускаем горутины для установки маршрутов
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			listener.setLastRoute(id, uint32(id), uint64(id*10))
		}(int64(i))
	}

	wg.Wait()

	// Проверяем несколько маршрутов
	for i := int64(0); i < 10; i++ {
		userID, dialogID, ok := listener.getLastRoute(i)
		if !ok {
			t.Errorf("Route %d should exist", i)
		}
		if userID != uint32(i) {
			t.Errorf("Expected userID %d, got %d", i, userID)
		}
		if dialogID != uint64(i*10) {
			t.Errorf("Expected dialogID %d, got %d", i*10, dialogID)
		}
	}
}
