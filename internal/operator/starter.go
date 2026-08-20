package operator

import (
	"air_operator/internal/metrics"
	"fmt"
	"time"

	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

func (l *Listener) StartOperators() error {
	logger.Infoln("Создание структуры операторов...")
	err := l.GetOperators(false)
	if err != nil {
		return fmt.Errorf("ошибка получения пользователей: %w", err)
	}

	ticker := time.NewTicker(40 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			logger.Info("Остановка цикла StartOperators по ctx.Done()")
			return nil
		case <-ticker.C:
			if err = l.GetOperators(true); err != nil {
				return fmt.Errorf("ошибка получения списка операторов: %w", err)
			}
		}
	}
}

// GetOperators получает список операторов из БД и обновляет operators OperatorsMap
// Логика:
//   - Если TelegramEnabled = true: добавляем все Telegram ID в карту (если новых ID ещё нет)
//   - Если TelegramEnabled = false: завершаем все активные диалоги для всех Telegram ID пользователя.
//     Для каждого активного диалога отправляем командное сообщение "Set-Mode-To-AI" в RX сессии
//     (аналогично confirmEndBtn) и удаляем диалог/telegram ID.
//   - Widget и WidgetEnabled пока игнорируются.
func (l *Listener) GetOperators(changed bool) error {
	// Получаем данные о пользователях из БД
	data, err := l.db.GetOperators(l.ctx, changed)
	if err != nil {
		metrics.ObserveOperatorSync("db_error")
		return fmt.Errorf("ошибка получения данных операторов: %w", err)
	}

	// Если изменений нет или данные пусты, просто выходим
	if len(data) == 0 {
		metrics.ObserveOperatorSync("empty")
		return nil
	}

	for _, op := range data {
		if op.TelegramEnabled {
			// Добавляем/гарантируем наличие всех telegram id
			existingTGs := l.operators.GetTGs(op.UserId)
			for _, tgID := range op.Telegram {
				if tgID == 0 {
					continue
				}

				// Проверяем, есть ли уже этот оператор
				alreadyExists := false
				for _, existingTG := range existingTGs {
					if existingTG == tgID {
						alreadyExists = true
						break
					}
				}

				l.operators.AddTG(op.UserId, tgID)

				if !alreadyExists {
					logger.Info("Добавлен оператор TG ID %d", tgID, op.UserId)
				}
			}
			continue
		}

		// TelegramEnabled = false -> завершаем все активные диалоги пользователя
		existingTGs := l.operators.GetTGs(op.UserId)
		if len(existingTGs) == 0 {
			continue // Нечего отключать
		}

		logger.Info("Удаление операторов: TG IDs %v", existingTGs, op.UserId)

		for _, tgID := range existingTGs {
			dialogs := l.operators.GetDialogs(op.UserId, tgID)
			for _, dialogID := range dialogs {
				// Получаем сессию (может отсутствовать если уже удалена конкурентно)
				sess := l.operators.GetSession(op.UserId, tgID, dialogID)
				if sess != nil {
					// Формируем команду завершения диалога (аналог confirmEndBtn)
					cmd := model.Message{
						Type:      "command",
						Content:   model.AssistResponse{Message: "Set-Mode-To-AI"},
						Name:      fmt.Sprintf("id:%d", tgID),
						Timestamp: time.Now(),
					}
					// Помечаем как операторское для единообразия с DeliverIncomingFromTG
					cmd.Operator = model.Operator{Operator: true, SetOperator: true}
					select {
					case sess.rx <- cmd:
					default:
					}
				}
				// Удаляем сам диалог
				l.operators.RemoveDialog(op.UserId, tgID, dialogID)
			}

			// Удаляем TG целиком
			l.operators.RemoveTG(op.UserId, tgID)
			logger.Info("Удален оператор TG ID %d", tgID, op.UserId)

			// Сбрасываем маршрут если он указывал на этот tgID
			if uid, _, ok := l.getLastRoute(tgID); ok && uid == op.UserId {
				l.lastRoute.Delete(tgID)
			}
		}
	}

	metrics.ObserveOperatorSync("success")
	return nil
}
