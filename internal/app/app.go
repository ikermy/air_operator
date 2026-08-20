package app

import (
	"air_operator/internal/db"
	httpdelivery "air_operator/internal/delivery/http"
	"air_operator/internal/domain"
	"air_operator/internal/operator"
	"air_operator/internal/telegram"
	"context"
	"fmt"

	"github.com/ikermy/air_common/pkg/com"
	"github.com/ikermy/air_common/pkg/endpoint"
	"github.com/ikermy/air_common/pkg/rpc"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

type End interface {
	Shutdown(shutCh chan<- com.LogMsg)
	NotificationListener(notifCh chan<- com.LogMsg)
}

type Telega interface {
	RunMarusiaAiOperatorBot()
}

type Listener interface {
	Listener()
	StartOperators() error
}

type App struct {
	ctx    context.Context
	cancel context.CancelFunc

	End      End
	Telega   Telega
	Listener Listener
}

func New(parent context.Context) *App {
	// Локальный дочерний контекст для уровня app
	ctx, cancel := context.WithCancel(parent)

	d, err := db.New(ctx)
	if err != nil {
		logger.Fatal("Ошибка инициализации базы данных: %v", err)
	}

	rpcClient, err := rpc.New()
	if err != nil {
		logger.Fatal(fmt.Errorf("ошибка создания rpc клиента: %w", err))
	}

	// Инжектируем resolver в comdb.DB
	//    Каждый раз когда DB-методу нужен MasterKey — он делает gRPC-запрос к AiR_ORCHESTRATOR
	d.SetMasterKeyResolver(func(userId uint32) ([32]byte, bool) {
		mk, err := rpcClient.GetUserMasterKey(context.Background(), userId)
		if err != nil {
			// codes.Unavailable — пользователь не логинился после рестарта AiR_ORCHESTRATOR
			// codes.Unauthenticated / PermissionDenied — неверный SERVICE_KEY
			return [32]byte{}, false
		}
		return mk, true
	})

	e := endpoint.New(ctx, d)

	// Получаем конфигурацию Telegram-бота через универсальный bff клиент
	operBotConfig, err := rpcClient.GetOperBotConfig(ctx)
	if err != nil {
		logger.Fatal("Ошибка получения конфигурации бота через gRPC: %w", err)
	}

	t := telegram.New(ctx, operBotConfig)
	l := operator.New(ctx, d, t, operBotConfig.BotName)
	h := httpdelivery.New(l)
	return &App{
		ctx:    ctx,
		cancel: cancel,

		End:      e,
		Telega:   t,
		Listener: h,
	}
}

func (a *App) Run() {
	readyCh := make(chan string)
	// Сначала запускаю бота MarusiaAiOperatorBot
	operBot := a.Telega.(*telegram.Telega)
	operBot.SetReadyChannel(readyCh)

	// Создаю шину для логирования сообщений от модулей
	bus := com.NewBus(10)

	go a.Telega.RunMarusiaAiOperatorBot()

	logger.Infoln(<-readyCh)
	close(readyCh) // Закрываю канал после получения сигнала готовности

	// читатель
	go uReader(bus.MsgCh)

	// Затем запускаю слушателя сообщений для операторов
	go a.Listener.Listener()

	// Запускаю операторов
	if err := a.Listener.StartOperators(); err != nil {
		logger.Fatalf("App: ошибка запуска операторов: %v", err)
		// В случае ошибки завершаю приложение
		a.cancel()
		return
	}

	// Обработка сигнала завершения
	go func() {
		<-a.ctx.Done()
		logger.Info("'App': получен сигнал завершения, начинаю shutdown")
		bus.Add(func(ch chan<- com.LogMsg) { a.End.Shutdown(ch) })

		logger.Info("App: все модули завершены, отправляю сигнал завершения БД")
		// ждём всех producers и закрываем канал
		bus.WaitAndClose()
		// Закрываю соединение с БД
		close(domain.UsersDB)
	}()
}

func uReader(readCh <-chan com.LogMsg) {
	for info := range readCh {
		switch info.Log {
		case 0: // Info
			logger.Info("%s: %v", info.Mod, info.Msg, info.UID)
		case 1: // Info
			logger.Error("%s: %v", info.Mod, info.Msg, info.UID)
		case 2: // Info
			logger.Warn("%s: %v", info.Mod, info.Msg, info.UID)
		case 3: // Info
			logger.Debug("%s: %v", info.Mod, info.Msg, info.UID)
		}
	}
}
