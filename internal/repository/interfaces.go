package repository

import (
	"air_operator/internal/domain"
	"context"
	"encoding/json"

	"github.com/ikermy/air_common/pkg/comdb"
)

// InternalRepository внутренние методы работы с БД
type InternalRepository interface {
	DialogLastMessages(dialogId uint64, limit uint8) (json.RawMessage, error)
	GetOperators(ctx context.Context, filterChanged bool) ([]domain.OperatorChannels, error)
}

// ExternalDBRepository интерфейс для внешних методов БД (из AiR_Common)
type ExternalDBRepository interface {
	comdb.Exterior
}

// Repository объединяет все репозитории
type Repository struct {
	Internal InternalRepository
	External ExternalDBRepository
}
