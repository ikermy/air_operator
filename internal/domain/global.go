package domain

// CarpCh - канал для передачи уведомлений
type CarpCh struct {
	TelegaID int64
	Message  string
}

var (
	UsersDB = make(chan struct{}) // Канал уведомления о завершении операций пользователями ДБ
	Exit    = make(chan struct{}) // Канал завершения работы приложения

	HistoryLimitMessages uint8 = 20 // HISTORY_LIMIT_MESSAGES
)
