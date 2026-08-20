# AiR Operator

![air_operator](air_operator_logo.png)

[🇬🇧 English version](README.md)

![Версия Go](https://img.shields.io/badge/Go-1.25.8-00ADD8?logo=go)
![Лицензия](https://img.shields.io/badge/license-MIT-blue)
[![Telegram](https://img.shields.io/badge/Telegram-Join%20Chat-blue?logo=telegram)](https://t.me/marusia_dev)

Операторский микросервис платформы AiR. Сервис связывает клиентский интерфейс с операторами в Telegram: принимает сообщения пользователя, передаёт их оператору и возвращает ответы обратно через Server-Sent Events (SSE).

## Возможности

- запуск Telegram-бота операторского режима;
- синхронизация списка активных операторов из MySQL;
- передача сообщений между клиентом и оператором;
- потоковая доставка ответов оператора через SSE;
- передача текста, команд и файлов в Telegram;
- завершение диалога и возврат пользователя в AI-режим;
- получение истории диалога для операторской сессии;
- Prometheus-метрики, структурированное логирование и корректное завершение работы.

## Архитектура

```text
Клиент / AI-контур
        |
        | HTTP: SSE / JSON
        v
  air_operator (:8080)
        |                |
        |                +--> Telegram Bot API
        |
        +--> MySQL
        |
        +--> air_orchestrator (gRPC)
             конфигурация бота и MasterKey
```

При запуске сервис получает конфигурацию Telegram-бота через gRPC-клиент `air_orchestrator`, подключается к MySQL и загружает активных операторов. Затем список операторов обновляется примерно каждые 40 секунд или при изменениях в базе данных.

## HTTP API

HTTP-сервер слушает порт `8080`.

```text
GET  /metrics
GET  /oper/available
GET  /op?user_id={user_id}&dialog_id={dialog_id}
POST /op
```

`GET /op` открывает SSE-соединение для конкретного пользователя и диалога. События включают сообщения оператора и служебные события завершения сессии.

`POST /op` принимает JSON:

```json
{
  "user_id": 123,
  "dialog_id": 456,
  "sid": 789,
  "msg": {
    "type": "user",
    "content": {}
  }
}
```

Полное описание API находится в [`doc/openapi.yaml`](doc/openapi.yaml).

## Технологии

- Go `1.25.8`;
- стандартный пакет `net/http`;
- MySQL;
- gRPC для взаимодействия с `air_orchestrator`;
- Telegram Bot API через `gopkg.in/telebot.v4`;
- Server-Sent Events (SSE);
- Prometheus;
- Docker и Docker Compose;
- `air_common` — общие модели, конфигурация, RPC и работа с БД;
- `air_logger` — логирование.

## Запуск

Для разработки:

```bash
docker compose -f dev.yml up --build
```

Для production:

```bash
docker compose -f prod.yml up -d
```

Сервис подключается к внешним Docker-сетям `air_shared` и `monitoring_shared`. В сети должны быть доступны MySQL под именем `air_db` и gRPC-сервис `air_orchestrator` под именем `airorc`.

## Конфигурация

Основные переменные окружения:

```text
DB_HOST=air_db:3306
DB_NAME=air
DB_USER
DB_PASSWORD
GRPC_CONFIG_HOST=airorc:50051
SERVICE_KEY_FILE=/run/secrets/service_key
HISTORY_LIMIT_MESSAGES=20
LOG_LEVEL=info
REAL_URL
```

`SERVICE_KEY_FILE` указывает на файл сервисного ключа, который используется для защищённого взаимодействия с `air_orchestrator`. Конфигурация Telegram-бота загружается через gRPC и не задаётся напрямую в Docker Compose.

## Структура проекта

- `cmd/main.go` — точка входа;
- `internal/app` — инициализация и жизненный цикл приложения;
- `internal/operator` — операторские сессии, SSE и маршрутизация сообщений;
- `internal/telegram` — Telegram-бот;
- `internal/db` и `internal/repository/mysql` — доступ к данным;
- `internal/delivery/http` — HTTP-сервер;
- `internal/metrics` — Prometheus-метрики;
- `doc/openapi.yaml` — спецификация HTTP API.


## Связанные сервисы
- [air_common](https://github.com/ikermy/air_common) — Общая библиотека для AI‑микросервисов
- [air_orchestrator](https://github.com/ikermy/air_orchestrator) — Главный сервис оркестратор
- [marusia_crm](https://github.com/ikermy/marusia_crm) — Сервис интеграции с внешними CRM системами
- [air_logger](https://github.com/ikermy/air_logger) — Вспомогательный сервис логирования событий с поддержкой многопользовательского режима и поддержкой сборщика логов loki

## Лицензия

Проект распространяется по лицензии [MIT](LICENSE). Она разрешает свободно использовать, копировать, изменять и распространять программное обеспечение при сохранении текста лицензии и уведомления об авторских правах.

Полный текст лицензии доступен в файле [`LICENSE`](LICENSE).

## Контакты
[![Telegram](https://img.shields.io/badge/Telegram-Contact-blue?logo=telegram)](https://t.me/ikermy)

