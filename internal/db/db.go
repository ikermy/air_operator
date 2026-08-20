package db

import (
	"air_operator/internal/domain"
	"air_operator/internal/repository"
	"air_operator/internal/repository/mysql"
	"context"
	"encoding/json"

	"github.com/ikermy/air_common/pkg/comdb"
	"github.com/ikermy/air_logger/v2/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
)

// DB обёртка соединения с базой данных и репозиториями
type DB struct {
	*comdb.DB
	repo repository.Repository
}

//func (d *DB) DialogLastMessages(ctx context.Context, dialogId uint64, limit uint16) (json.RawMessage, error) {
//	return d.repo.Internal.DialogLastMessages(ctx, dialogId, limit)
//}

func (d *DB) DialogLastMessages(dialogId uint64, limit uint8) (json.RawMessage, error) {
	return d.repo.Internal.DialogLastMessages(dialogId, limit)
}

func (d *DB) GetOperators(ctx context.Context, filterChanged bool) ([]domain.OperatorChannels, error) {
	return d.repo.Internal.GetOperators(ctx, filterChanged)
}

// New создаёт подключение к БД и инициализирует репозитории
func New(parent context.Context) (*DB, error) {
	base, err := comdb.New(parent)
	if err != nil {
		return nil, err
	}
	repo, err := mysql.New(base)
	if err != nil {
		return nil, err
	}
	return &DB{
		DB:   base,
		repo: repo,
	}, nil
}

// Repo возвращает набор репозиториев
func (d *DB) Repo() repository.Repository {
	return d.repo
}

// HandlerClose ожидает завершения всех операций с БД и закрывает соединение
func (d *DB) HandlerClose() {
	go func() {
		<-d.MainCTX().Done()
		logger.Info("DB: контекст отменен, ожидаю завершения всех операций...")
		<-domain.UsersDB
		logger.Info("DB: все модули завершили работу, закрываю соединение...")
		if err := d.Close(); err != nil {
			logger.Error("DB: ошибка при закрытии: %v", err)
		}
		close(domain.Exit)
	}()
}
