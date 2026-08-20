package domain

type OperatorChannels struct {
	UserId          uint32
	Telegram        []int64 // Список telegram id
	TelegramEnabled bool
	Widget          []uint64 // Список widget id
	WidgetEnabled   bool
}
