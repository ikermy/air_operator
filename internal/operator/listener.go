package operator

import (
	"air_operator/internal/db"
	"air_operator/internal/metrics"
	"air_operator/internal/repository"
	"air_operator/internal/telegram"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

// Локальный генератор случайных чисел вместо устаревшего глобального Seed
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// Telega интерфейс отправки сообщений в Telegram
type Telega interface {
	SendHistory(recipient int64, raw []byte) error
	SendMsg(recipient int64, message model.Message) (int, error)
}

// SessionInfo хранит информацию о сессии SSE для конкретного диалога
type SessionInfo struct {
	tx         chan model.Message
	rx         chan model.Message
	lastActive atomic.Pointer[time.Time]
}

// UpdateLastActive обновляет время последней активности сессии на текущее
func (s *SessionInfo) updateLastActive() {
	now := time.Now()
	s.lastActive.Store(&now)
}

// GetLastActive возвращает время последней активности сессии
func (s *SessionInfo) GetLastActive() time.Time {
	if ptr := s.lastActive.Load(); ptr != nil {
		return *ptr
	}
	return time.Time{} // zero value если не установлено
}

// IsExpired проверяет, истекло ли время сессии, сравнивая с таймаутом
func (s *SessionInfo) IsExpired(timeout time.Duration) bool {
	return time.Since(s.GetLastActive()) > timeout
}

// OperatorsMap — потокобезопасная структура:
// users[userID][tgID][dialogID] => *SessionInfo
type OperatorsMap struct {
	mu    sync.RWMutex
	users map[uint32]map[int64]map[uint64]*SessionInfo
}

func (o *OperatorsMap) reportMetricsLocked() {
	dialogs := 0
	operators := 0
	for _, tgMap := range o.users {
		operators += len(tgMap)
		for _, dlgMap := range tgMap {
			dialogs += len(dlgMap)
		}
	}
	metrics.SetActiveDialogs(dialogs)
	metrics.SetActiveOperators(operators)
}

// AddTG гарантирует, что у userID существует запись для tgID
func (o *OperatorsMap) AddTG(userID uint32, tgID int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.users == nil {
		o.users = make(map[uint32]map[int64]map[uint64]*SessionInfo)
	}
	if _, ok := o.users[userID]; !ok {
		o.users[userID] = make(map[int64]map[uint64]*SessionInfo)
	}
	if _, ok := o.users[userID][tgID]; !ok {
		o.users[userID][tgID] = make(map[uint64]*SessionInfo)
	}
	o.reportMetricsLocked()
}

// AddSession создаёт/заменяет канал SSE для (userID,tgID,dialogID) и возвращает его.
// Если существовал старый канал, он будет закрыт.
func (o *OperatorsMap) AddSession(userID uint32, tgID int64, dialogID uint64) *SessionInfo {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.users == nil {
		o.users = make(map[uint32]map[int64]map[uint64]*SessionInfo)
	}
	if _, ok := o.users[userID]; !ok {
		o.users[userID] = make(map[int64]map[uint64]*SessionInfo)
	}
	if _, ok := o.users[userID][tgID]; !ok {
		o.users[userID][tgID] = make(map[uint64]*SessionInfo)
	}

	old := o.users[userID][tgID][dialogID]
	session := &SessionInfo{
		tx: make(chan model.Message, 1),
		rx: make(chan model.Message, 1),
	}
	session.updateLastActive()
	o.users[userID][tgID][dialogID] = session

	if old != nil {
		close(old.tx)
		close(old.rx)
	}
	o.reportMetricsLocked()
	return session
}

// GetSession возвращает данные сессии SSE для (userID,tgID,dialogID)
func (o *OperatorsMap) GetSession(userID uint32, tgID int64, dialogID uint64) *SessionInfo {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.users == nil {
		return nil
	}
	if tgMap, ok := o.users[userID]; ok {
		if dlgMap, ok2 := tgMap[tgID]; ok2 {
			return dlgMap[dialogID]
		}
	}
	return nil
}

// RemoveSession удаляет диалог, только если текущий канал совпадает с переданным
func (o *OperatorsMap) RemoveSession(userID uint32, tgID int64, dialogID uint64, info *SessionInfo) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.users == nil {
		return false
	}
	if tgMap, ok := o.users[userID]; ok {
		if dlgMap, ok2 := tgMap[tgID]; ok2 {
			cur := dlgMap[dialogID]
			if cur == info {
				if cur != nil {
					close(cur.tx)
					close(cur.rx)
				}
				delete(dlgMap, dialogID)
				o.reportMetricsLocked()
				return true
			}
		}
	}
	return false
}

// RemoveDialog удаляет dialogID для конкретного tgID пользователя (безусловно).
func (o *OperatorsMap) RemoveDialog(userID uint32, tgID int64, dialogID uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.users == nil {
		return
	}
	if tgMap, ok := o.users[userID]; ok {
		if dlgMap, ok2 := tgMap[tgID]; ok2 {
			if cur := dlgMap[dialogID]; cur != nil {
				close(cur.tx)
				close(cur.rx)
			}
			delete(dlgMap, dialogID)
			o.reportMetricsLocked()
		}
	}
}

// RemoveTG удаляет tgID (и все его dialogID) у пользователя
func (o *OperatorsMap) RemoveTG(userID uint32, tgID int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.users == nil {
		return
	}
	if tgMap, ok := o.users[userID]; ok {
		if dlgMap, ok2 := tgMap[tgID]; ok2 {
			for _, info := range dlgMap {
				if info != nil {
					close(info.tx)
					close(info.rx)
				}
			}
			delete(tgMap, tgID)
			o.reportMetricsLocked()
		}
	}
}

// DeleteUser полностью удаляет пользователя со всеми его tgID и dialogID
func (o *OperatorsMap) DeleteUser(userID uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.users == nil {
		return
	}
	if tgMap, ok := o.users[userID]; ok {
		for _, dlgMap := range tgMap {
			for _, info := range dlgMap {
				if info != nil {
					close(info.tx)
					close(info.rx)
				}
			}
		}
		delete(o.users, userID)
		o.reportMetricsLocked()
	}
}

// GetTGs возвращает список tgID по userID
func (o *OperatorsMap) GetTGs(userID uint32) []int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.users == nil {
		return nil
	}
	if tgMap, ok := o.users[userID]; ok {
		out := make([]int64, 0, len(tgMap))
		for tgID := range tgMap {
			out = append(out, tgID)
		}
		return out
	}
	return nil
}

// GetTg возвращает tgID по userID:
// 1) если есть несколько tgID без активных диалогов – случайный из них
// 2) иначе случайный среди tgID с минимальным числом диалогов
func (o *OperatorsMap) GetTg(userID uint32) int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.users == nil {
		return 0
	}
	tgMap, ok := o.users[userID]
	if !ok || len(tgMap) == 0 {
		return 0
	}

	var idle []int64 // tgID без диалогов
	minCount := -1
	var minIDs []int64 // tgID с минимальным числом диалогов (>0)

	for tgID, dlgMap := range tgMap {
		count := len(dlgMap)
		if count == 0 {
			idle = append(idle, tgID)
			continue
		}
		if minCount == -1 || count < minCount {
			minCount = count
			minIDs = minIDs[:0]
			minIDs = append(minIDs, tgID)
		} else if count == minCount {
			minIDs = append(minIDs, tgID)
		}
	}

	if len(idle) > 0 {
		return idle[rng.Intn(len(idle))]
	}
	if len(minIDs) > 0 {
		return minIDs[rng.Intn(len(minIDs))]
	}
	return 0
}

// GetDialogs возвращает список dialogID по паре (userID, tgID)
func (o *OperatorsMap) GetDialogs(userID uint32, tgID int64) []uint64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.users == nil {
		return nil
	}
	if tgMap, ok := o.users[userID]; ok {
		if dlgMap, ok2 := tgMap[tgID]; ok2 {
			out := make([]uint64, 0, len(dlgMap))
			for dlgID := range dlgMap {
				out = append(out, dlgID)
			}
			return out
		}
	}
	return nil
}

// HasDialog проверяет наличие dialogID у пары (userID, tgID)
func (o *OperatorsMap) HasDialog(userID uint32, tgID int64, dialogID uint64) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.users == nil {
		return false
	}
	if tgMap, ok := o.users[userID]; ok {
		if dlgMap, ok2 := tgMap[tgID]; ok2 {
			_, ok3 := dlgMap[dialogID]
			return ok3
		}
	}
	return false
}

// UpdateLastActive обновляет время последней активности сессии на текущее
func (o *OperatorsMap) UpdateLastActive(userID uint32, tgID int64, dialogID uint64) {
	if session := o.GetSession(userID, tgID, dialogID); session != nil {
		session.updateLastActive()
	}
}

type Listener struct {
	ctx    context.Context
	cancel context.CancelFunc

	db             repository.InternalRepository
	carpinteroName string

	// Карта для хранения всех Telegram ID и диалогов операторов и их SSE-сессий
	operators OperatorsMap

	// Отправитель в Telegram
	tg Telega

	// Маршрутизация входящих из TG: tgID -> (userID, dialogID)
	lastRoute sync.Map // key int64 -> struct{userID uint32; dialogID uint64}
}

func New(parent context.Context, d *db.DB, t *telegram.Telega, botName string) *Listener {
	ctx, cancel := context.WithCancel(parent)

	l := &Listener{
		ctx:            ctx,
		cancel:         cancel,
		db:             d,
		tg:             t,
		carpinteroName: botName,
	}

	// Пробрасываем входящие из Telegram сразу в операторский слой
	t.SetIncomingHandler(l.DeliverIncomingFromTG)
	t.SetCloseSSE(l.CloseSSE)

	return l
}

// SetTelegramSender регистрирует отправителя сообщений в Telegram
func (l *Listener) SetTelegramSender(sender Telega) { l.tg = sender }

// setLastRoute сохраняет последнюю активную связку для tgID
func (l *Listener) setLastRoute(tgID int64, userID uint32, dialogID uint64) {
	l.lastRoute.Store(tgID, struct {
		userID   uint32
		dialogID uint64
	}{userID: userID, dialogID: dialogID})
}

// getLastRoute получает связку для tgID
func (l *Listener) getLastRoute(tgID int64) (uint32, uint64, bool) {
	if v, ok := l.lastRoute.Load(tgID); ok {
		p := v.(struct {
			userID   uint32
			dialogID uint64
		})
		return p.userID, p.dialogID, true
	}
	return 0, 0, false
}

// DeliverIncomingFromTG доставляет входящее сообщение Telegram в rx конкретного диалога,
// который последним отправлял сообщение этому tgID (lastRoute).
func (l *Listener) DeliverIncomingFromTG(tgID int64, msg model.Message) {
	userID, dialogID, ok := l.getLastRoute(tgID)
	if !ok {
		metrics.ObserveMessageEvent("telegram", "no_route", msg.Type)
		logger.Warn("Нет маршрута для входящего сообщения tgID=%d", tgID)
		return
	}

	sess := l.operators.GetSession(userID, tgID, dialogID)
	if sess == nil {
		metrics.ObserveMessageEvent("telegram", "no_session", msg.Type)
		logger.Warn("Сессия не найдена для входящего сообщения: user=%d tg=%d dialog=%d", userID, tgID, dialogID)
		return
	}

	// Помечаем сообщение как от оператора и с установкой операторского режима
	msg.Operator = model.Operator{
		Operator:    true,
		SetOperator: true,
	}
	select {
	case sess.rx <- msg:
		metrics.ObserveMessageEvent("telegram", "routed", msg.Type)
	default:
		// не блокируем, если канал переполнен
		metrics.ObserveMessageEvent("telegram", "dropped", msg.Type)
	}
	l.operators.UpdateLastActive(userID, tgID, dialogID)
}

// operatorLayerListener слушает канал сообщений для оператора и отправляет их в Телеграм
// также обновляет lastRoute для tgID, чтобы входящие сообщения маршрутизировались
func (l *Listener) operatorLayerListener(ctx context.Context, session *SessionInfo, userID uint32, tgID int64, dialogID uint64) {
	if l.tg == nil {
		logger.Warn("Отправитель Telegram не установлен, пропуск отправки")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-session.tx:
			if !ok {
				return
			}
			startedAt := time.Now()
			if _, err := l.tg.SendMsg(tgID, msg); err != nil {
				metrics.ObserveTelegramSend("message", "error", startedAt)
				metrics.ObserveMessageEvent("operator", "send_error", msg.Type)
				logger.Warn("Ошибка отправки в Telegram: %v", err)
				continue
			}
			metrics.ObserveTelegramSend("message", "success", startedAt)
			metrics.ObserveMessageEvent("operator", "sent", msg.Type)
			// Связываю этого пользователя телеги с диалогом только после успешной отправки
			l.setLastRoute(tgID, userID, dialogID)
			l.operators.UpdateLastActive(userID, tgID, dialogID)
		}
	}
}

// CloseSSE закрывает SSE-сессию для tgID, только если у него единственный активный диалог
func (l *Listener) CloseSSE(tgID int64) {
	//time.Sleep(500 * time.Millisecond) // Небольшая задержка, чтобы не прервать сразу после отправки

	// Получаем последний маршрут для этого tgID
	userID, dialogID, ok := l.getLastRoute(tgID)
	if !ok {
		logger.Info("Нет активного маршрута для tgID=%d", tgID)
		return
	}

	// Получаем все диалоги для данной пары (userID, tgID)
	dialogs := l.operators.GetDialogs(userID, tgID)

	// Проверяем, что диалог единственный
	if len(dialogs) != 1 {
		logger.Info("У tgID=%d найдено %d диалогов, закрытие SSE отменено", tgID, len(dialogs))
		return
	}

	// Проверяем, что единственный диалог соответствует последнему маршруту
	if dialogs[0] == dialogID {
		logger.Info("Закрытие SSE-сессии для user=%d, tg=%d, dialog=%d", userID, tgID, dialogID)
		l.operators.RemoveDialog(userID, tgID, dialogID)
		// Удаляем маршрут после закрытия диалога
		l.lastRoute.Delete(tgID)
	} else {
		logger.Warn("Несоответствие dialogID в маршруте (%d) и активном диалоге (%d) для tgID=%d", dialogID, dialogs[0], tgID)
	}
}
