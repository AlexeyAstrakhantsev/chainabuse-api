# ChainAbuse API Parser

Сервис для сбора и хранения данных из API chainabuse.com

## Установка и запуск

### Предварительные требования

- Docker и Docker Compose
- API-токен для chainabuse.com

### Шаги по установке

1. Клонируйте репозиторий:
```bash
git clone https://github.com/yourusername/chainabuse-api.git
cd chainabuse-api
```

2. Создайте файл `.env` и настройте переменные окружения:
```bash
cp .env.example .env
# Отредактируйте файл .env, указав ваши настройки
```

3. Запустите сервис:
```bash
docker-compose up -d
```

## Использование

Сервис предоставляет следующие API-эндпоинты:

- `POST /fetch_reports` - Запуск сбора данных из API chainabuse.com
- `POST /fetch_reports_background` - Запуск сбора данных в фоновом режиме
- `GET /status` - Проверка статуса сервиса и получение статистики

## Автоматическое обновление

По умолчанию сервис обновляет данные каждый час. Вы можете изменить интервал обновления, указав переменную окружения `UPDATE_INTERVAL_MINUTES`.

## Доступ к данным

Данные хранятся в PostgreSQL базе данных. Вы можете получить доступ к ним, используя любой PostgreSQL-клиент:

```bash
psql -h localhost -U postgres -d chainabuse
```

Основные таблицы:
- `reports` - Содержит информацию об отчетах
- `report_addresses` - Содержит адреса, связанные с отчетами 