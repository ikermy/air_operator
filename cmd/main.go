package main

import (
	"air_operator/internal/app"
	"air_operator/internal/domain"
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ikermy/air_common/pkg/com"
	"github.com/ikermy/air_common/pkg/mode"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

func main() {
	// Инициализируем инфраструктурные переменные из env vars (порты, домен, TTL, логи).
	// Все значения имеют разумные дефолты; некорректные критичные — fatal.
	mode.InitFromEnv(logger.Fatalf)

	// Логгер: режим os.Stdout для Docker
	logSetup := logger.StdOut()
	// Можно установить через mode.SetLogLevel иначе установится из env в InitFromEnv
	// Если не устанавливать ничего = info
	// Уровень логирования читается из env.LOG_LEVEL
	logSetup.WithLogLevel(logSetup.FromString(mode.GetLogLevel()))
	logSetup.Apply()

	logger.Debug(com.GetVersionInfo())

	messageLimit, err := strconv.Atoi(os.Getenv("HISTORY_LIMIT_MESSAGES"))
	if err != nil {
		logger.Warn("Некорректное значение HISTORY_LIMIT_MESSAGES: %v, используем значение по умолчанию", err)
	} else {
		domain.HistoryLimitMessages = uint8(messageLimit)
	}

	// Корневой контекст процесса, отменяется по сигналам ОС
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	a := app.New(ctx)
	a.Run()

	// Ожидание завершения работы
	<-domain.Exit

	logger.Infoln("Приложение air_oper завершено")
}
