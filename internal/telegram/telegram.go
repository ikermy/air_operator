package telegram

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_common/pkg/rpc/proto"
	"github.com/ikermy/air_logger/v2/pkg/logger"
	tele "gopkg.in/telebot.v4"
)

// Sender структура для хранения информации об отправителе сообщения для использования в канале сообщений для оператора
type Sender struct {
	Time    time.Time
	Message tele.Message
}

type Telega struct {
	ctx    context.Context
	cancel context.CancelFunc

	token   string
	botName string
	botID   string

	b        *tele.Bot
	botReady chan string    // Канал для уведомления о готовности бота
	re429    *regexp.Regexp // Ошибка 429 ТГ лимит отправки сообщений

	// колбэк для проброса входящих сообщений в операторский слой
	onIncoming func(tgID int64, msg model.Message)
	closeSSE   func(tgID int64)
}

func New(parent context.Context, botConfig *proto.BotConfigResponse) *Telega {
	ctx, cancel := context.WithCancel(parent)

	// Если конфиг не получен, используем пустые значения
	token := ""
	botName := ""
	botID := ""
	if botConfig != nil {
		token = botConfig.Token
		botName = botConfig.BotName
		if idx := strings.Index(token, ":"); idx != -1 {
			botID = token[:idx]
		}
	}
	return &Telega{
		ctx:    ctx,
		cancel: cancel,

		botReady: make(chan string),
		b:        nil,
		token:    token,
		botName:  botName,
		botID:    botID,
		re429:    regexp.MustCompile(`retry after \d+ \(429\)`),
	}
}

// SetReadyChannel устанавливает канал для уведомления о готовности бота
func (t *Telega) SetReadyChannel(ch chan string) {
	t.botReady = ch
}

// SetIncomingHandler регистрирует обработчик входящих сообщений от Telegram
func (t *Telega) SetIncomingHandler(h func(tgID int64, msg model.Message)) { t.onIncoming = h }

// SetCloseSSE регистрирует обработчик закрытия SSE соединения для вызова из телеги
func (t *Telega) SetCloseSSE(h func(tgID int64)) { t.closeSSE = h }

func (t *Telega) RunMarusiaAiOperatorBot() {
	// Проверяю что токен бота задан
	if t.token == "" {
		t.botReady <- "Токен бота не задан в конфигурации, MarusiaAiOperatorBot не запущен"
		return
	}

	// Иначе, создаю бота и запускаю его
	b, err := tele.NewBot(tele.Settings{
		Token:  t.token,
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	})

	if err != nil {
		logger.Fatalf("Ошибка запуска Carpintero %e", err)
	}
	t.b = b

	t.botReady <- fmt.Sprintf("Бот %s для взаимодействия с оператором запущен!", t.b.Me.Username)

	// Добавляем обработчик для команды /start сразу шлём ID пользователя
	t.b.Handle("/start", func(ctx tele.Context) error {
		_, err = t.SendUserID(t.ctx, ctx.Message())
		return err
	})

	// Добавляю слушатель пользовательских сообщений
	t.HandleUserMessages()

	// Жду завершения контекста
	go func() {
		<-t.ctx.Done()
		logger.Info("'Telegram': Получен сигнал завершения работы")
		t.b.Stop()
		t.cancel()
		logger.Info("'Telegram': Завершение работы")
	}()

	t.b.Start()
}

func (t *Telega) SendUserID(ctx context.Context, message *tele.Message) (int, error) {
	if message == nil || message.Sender == nil {
		return -1, nil
	}

	select {
	case <-ctx.Done():
		logger.Info("SendUserID cancelled due to context")
		return -1, ctx.Err()
	default:
	}

	tgId := message.Sender.ID
	msg := model.Message{
		Content: model.AssistResponse{
			Message: "Telegram ID: " + strconv.FormatInt(tgId, 10),
		},
	}

	return t.SendMsg(tgId, msg)
}

// SendMsg отправляет сообщение и вложения пользователю Telegram.
// Теперь принимает model.Message и умеет отправлять файлы (по аналогии с otro.go).
func (t *Telega) SendMsg(recipient int64, message model.Message) (int, error) {
	if recipient == 0 {
		logger.Error("Recipient ID is zero, message not sent")
		return -1, nil
	}
	if t.b == nil {
		logger.Warn("Телеграм-бот ещё не инициализирован")
		return -1, fmt.Errorf("bot is not initialized")
	}

	var lastMsgID = -1

	// 1) Отправляем вложения, если есть
	for _, f := range message.Files {
		// Подготавливаем файл для Telegram
		tgFile := tele.File{FileReader: f.Content}

		var sendErr error
		var tgMsg *tele.Message

		mime := f.MimeType
		// Отправка по типу MIME
		switch {
		case len(mime) >= 6 && mime[:6] == "image/":
			photo := &tele.Photo{File: tgFile}
			tgMsg, sendErr = t.sendWithRetry(recipient, photo, nil)
		case len(mime) >= 6 && mime[:6] == "video/":
			video := &tele.Video{File: tgFile}
			tgMsg, sendErr = t.sendWithRetry(recipient, video, nil)
		case len(mime) >= 6 && mime[:6] == "audio/":
			audio := &tele.Audio{File: tgFile}
			tgMsg, sendErr = t.sendWithRetry(recipient, audio, nil)
		default:
			doc := &tele.Document{File: tgFile, FileName: f.Name}
			tgMsg, sendErr = t.sendWithRetry(recipient, doc, nil)
		}

		if sendErr != nil {
			logger.Warn("Failed to send attachment to Telegram: %v", sendErr)
			continue
		}
		if tgMsg != nil {
			lastMsgID = tgMsg.ID
		}
	}

	// 2) Отправляем текст, если есть
	text := message.Content.Message
	if text != "" {
		// Формируем текст с информацией об отправителе, если она есть
		fullText := text
		if message.Operator.SenderName != "" {
			fullText = fmt.Sprintf("Сообщение от %s\n\n%s", message.Operator.SenderName, text)
		}

		tgMsg, err := t.sendWithRetry(recipient, fullText, &tele.SendOptions{ParseMode: tele.ModeHTML, DisableNotification: true})
		if err != nil {
			logger.Warn("Failed to send message to Telegram after retries: %v", err)
			logger.Warn("Message: %s", text)
			logger.Warn("Recipient: %d", recipient)
			return lastMsgID, nil
		}
		if tgMsg != nil {
			lastMsgID = tgMsg.ID
		}
	}

	return lastMsgID, nil
}

// sendWithRetry выполняет отправку с повторными попытками и обработкой 429
func (t *Telega) sendWithRetry(recipient int64, payload any, opts *tele.SendOptions) (*tele.Message, error) {
	var tgMsg *tele.Message
	var err error
	for attempts := 1; attempts <= 3; attempts++ {
		tgMsg, err = t.b.Send(tele.ChatID(recipient), payload, opts)
		if err != nil {
			if attempts < 3 && t.re429.MatchString(err.Error()) {
				logger.Info("Failed to send to Telegram: %v, retrying in %d seconds...", err, 2*attempts)
				time.Sleep(time.Duration(2*attempts) * time.Second)
				continue
			}
			return nil, err
		}
		return tgMsg, nil
	}
	return nil, err
}

// HandleUserMessages регистрирует обработчик входящих текстовых сообщений
func (t *Telega) HandleUserMessages() {
	if t.b == nil {
		logger.Warn("Телеграм-бот ещё не инициализирован")
		return
	}

	// --- Основная клавиатура ---
	mainMarkup := &tele.ReplyMarkup{ResizeKeyboard: true}
	endDialogBtn := mainMarkup.Text("🔚 Завершить диалог")
	mainMarkup.Reply(
		mainMarkup.Row(endDialogBtn),
	)

	// --- Клавиатура подтверждения ---
	confirmMarkup := &tele.ReplyMarkup{ResizeKeyboard: true}
	confirmEndBtn := confirmMarkup.Text("✅ Завершить")
	cancelEndBtn := confirmMarkup.Text("❌ Отмена")
	confirmMarkup.Reply(
		confirmMarkup.Row(confirmEndBtn, cancelEndBtn),
	)

	// Обработка всех текстовых сообщений
	t.b.Handle(tele.OnText, func(c tele.Context) error {
		m := c.Message()
		username := m.Sender.Username
		if username == "" {
			username = fmt.Sprintf("id:%d", m.Sender.ID)
		}

		switch m.Text {
		case endDialogBtn.Text:
			// Пользователь нажал "Завершить диалог"
			return c.Send("Вы уверены, что хотите завершить диалог?", confirmMarkup)

		case confirmEndBtn.Text:
			// Подтверждение завершения
			if t.onIncoming != nil {
				msg := model.Message{
					Type: "command",
					Content: model.AssistResponse{
						Message: "Set-Mode-To-AI",
					},
					Name:      fmt.Sprintf("id:%d", c.Sender().ID),
					Timestamp: time.Now(),
				}
				t.onIncoming(c.Sender().ID, msg)

				t.closeSSE(c.Sender().ID)
			}
			// Убираем клавиатуру после завершения
			removeMarkup := &tele.ReplyMarkup{RemoveKeyboard: true}
			// Сообщение для оператора
			return c.Send("✅ Диалог завершен. Переключение на AI режим.", removeMarkup)

		case cancelEndBtn.Text:
			// Отмена завершения
			return c.Send("❌ Завершение диалога отменено.", mainMarkup)

		default:
			// Обычное сообщение пользователя
			if t.onIncoming != nil {
				msg := model.Message{
					Type: "user",
					Content: model.AssistResponse{
						Message: m.Text,
					},
					Name:      username,
					Timestamp: time.Now(),
				}
				t.onIncoming(m.Sender.ID, msg)
			}
			//return c.Send(fmt.Sprintf("Для %s ID: %d", username, m.Sender.ID), mainMarkup)
			return c.Send(fmt.Sprintf("Для %s", username), mainMarkup)
		}
	})
}
