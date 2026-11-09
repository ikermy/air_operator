package repository

import (
	"air_operator/internal/domain"
	"context"
	"encoding/json"

	"github.com/ikermy/air-common/v2/pkg/comdb"
)

// InternalRepository внутренние методы работы с БД
type InternalRepository interface {
	DialogLastMessages(ctx context.Context, dialogId uint64, limit uint16) (json.RawMessage, error)
	GetOperators(ctx context.Context, filterChanged bool) ([]domain.OperatorChannels, error)
}

// ExternalDBRepository интерфейс для внешних методов БД (из air-common)
type ExternalDBRepository interface {
	comdb.Exterior
}

// Repository объединяет все репозитории
type Repository struct {
	Internal InternalRepository
	External ExternalDBRepository
}
