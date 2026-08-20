package mysql

import (
	"air_operator/internal/domain"
	"air_operator/internal/repository"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ikermy/air_common/pkg/comdb"
	"github.com/ikermy/air_common/pkg/mode"
)

// Repository реализация интерфейса Repository для MySQL
type Repository struct {
	db *comdb.DB
}

func (r *Repository) DialogLastMessages(dialogId uint64, limit uint8) (json.RawMessage, error) {
	return r.db.ReadDialog(dialogId, limit)
}

// New создаёт новый MySQL репозиторий пользователей
func New(db *comdb.DB) (repository.Repository, error) {
	if db == nil {
		return repository.Repository{}, fmt.Errorf("database connection is nil")
	}

	userRepo := &Repository{
		db: db,
	}

	return repository.Repository{
		Internal: userRepo,
		External: db,
	}, nil
}

// GetOperators возвращает список операторов с их каналами уведомлений
// Если filterChanged=false – возвращаются все пользователи с хотя бы одним включённым каналом.
// Если filterChanged=true – возвращаются только изменённые, после чего в БД для них сбрасывается флаг Changed.
func (r *Repository) GetOperators(ctx context.Context, filterChanged bool) ([]domain.OperatorChannels, error) {
	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	var query string
	if filterChanged {
		query = `SELECT UserId, Telegram, Telegram_enabled, Widget, Widget_enabled 
		         FROM operators 
		         WHERE Changed = 1`
	} else {
		query = `SELECT UserId, Telegram, Telegram_enabled, Widget, Widget_enabled 
		         FROM operators 
		         WHERE Telegram_enabled = 1 OR Widget_enabled = 1`
	}

	rows, err := r.db.Conn().QueryContext(ctx, query)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при вызове GetOperators: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при вызове GetOperators: %w", err)
		default:
			return nil, fmt.Errorf("ошибка выполнения запроса GetOperators: %w", err)
		}
	}
	defer func() { _ = rows.Close() }()

	// Если нужно сбросить флаг Changed, делаем это после получения данных
	if filterChanged {
		_, err := r.db.Conn().ExecContext(ctx, `UPDATE operators SET Changed = 0, Timechange = NOW() WHERE Changed = 1`)
		if err != nil {
			return nil, fmt.Errorf("ошибка сброса флага Changed: %w", err)
		}
	}

	parseInt64Slice := func(src string) ([]int64, error) {
		s := strings.TrimSpace(src)
		if s == "" {
			return nil, nil
		}
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("невалидный JSON массива int64: %s", s)
		}
		var arr []int64
		if err := json.Unmarshal([]byte(s), &arr); err != nil {
			return nil, fmt.Errorf("ошибка разбора массива int64: %w", err)
		}
		return arr, nil
	}
	parseUint64Slice := func(src string) ([]uint64, error) {
		s := strings.TrimSpace(src)
		if s == "" {
			return nil, nil
		}
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("невалидный JSON массива uint64: %s", s)
		}
		var tmp []interface{}
		if err := json.Unmarshal([]byte(s), &tmp); err != nil {
			return nil, fmt.Errorf("ошибка разбора массива uint64: %w", err)
		}
		res := make([]uint64, 0, len(tmp))
		for i, v := range tmp {
			// json.Unmarshal в interface{} числа даёт float64
			f, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("элемент %d не число", i)
			}
			if f < 0 {
				return nil, fmt.Errorf("отрицательное значение в uint64 массиве index=%d", i)
			}
			res = append(res, uint64(f))
		}
		return res, nil
	}

	var result []domain.OperatorChannels
	for rows.Next() {
		var (
			userId                         uint32
			telegramStr, widgetStr         sql.NullString
			telegramEnabled, widgetEnabled sql.NullBool
		)

		if err := rows.Scan(&userId, &telegramStr, &telegramEnabled, &widgetStr, &widgetEnabled); err != nil {
			return nil, fmt.Errorf("не удалось прочитать строку GetOperators: %w", err)
		}

		op := domain.OperatorChannels{UserId: userId}
		if telegramEnabled.Valid {
			op.TelegramEnabled = telegramEnabled.Bool
		}
		if widgetEnabled.Valid {
			op.WidgetEnabled = widgetEnabled.Bool
		}
		if telegramStr.Valid && strings.TrimSpace(telegramStr.String) != "" {
			arr, err := parseInt64Slice(telegramStr.String)
			if err != nil {
				return nil, fmt.Errorf("userId=%d Telegram: %w", userId, err)
			}
			op.Telegram = arr
		}
		if widgetStr.Valid && strings.TrimSpace(widgetStr.String) != "" {
			arr, err := parseUint64Slice(widgetStr.String)
			if err != nil {
				return nil, fmt.Errorf("userId=%d Widget: %w", userId, err)
			}
			op.Widget = arr
		}

		result = append(result, op)
	}

	if err := rows.Err(); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при обработке результатов GetOperators: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при обработке результатов GetOperators: %w", err)
		default:
			return nil, fmt.Errorf("ошибка обхода строк GetOperators: %w", err)
		}
	}

	// Пустой результат НЕ является ошибкой — возвращаем пустой срез
	if result == nil {
		result = make([]domain.OperatorChannels, 0)
	}
	return result, nil
}
