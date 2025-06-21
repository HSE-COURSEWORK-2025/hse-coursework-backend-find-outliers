# HSE Coursework: Outliers Detection Backend

Этот репозиторий содержит сервис для поиска выбросов в пользовательских данных.

## Основные возможности
- Поиск выбросов в сырых и обработанных данных пользователя
- Определение выбросов по z-score
- Отправка уведомлений о начале и завершении анализа
- Хранение результатов в базе данных PostgreSQL

## Быстрый старт

### 1. Клонирование репозитория
```bash
git clone https://github.com/your-username/hse-coursework-backend-found-outliers.git
cd hse-coursework-backend-found-outliers
```

### 2. Сборка Docker-образа
```bash
docker build -f dockerfile.dag -t find_outliers:latest .
```

### 3. Запуск контейнера
```bash
docker run --env-file .env.prod find_outliers:latest --email <user@example.com>
```
Где `<user@example.com>` — email пользователя, для которого требуется провести анализ выбросов.

### 4. Переменные окружения
Используйте `.env.dev` для разработки и `.env.prod` для продакшена. Примеры переменных:
```
DATA_COLLECTION_API_BASE_URL=http://localhost:8082
AUTH_API_BASE_URL=http://localhost:8081
REDIS_HOST=localhost
NOTIFICATIONS_API_BASE_URL=http://localhost:8083/notifications-api/api/v1/notifications
```

### 5. Развёртывание в Kubernetes
Скрипт для развертывания:
```bash
./deploy.sh
```

## Структура проекта
- `run.py` — основной скрипт запуска поиска выбросов
- `db/` — модуль работы с базой данных (engine, session, схемы)
- `settings.py` — конфигурация приложения
- `notifications.py` — отправка email-уведомлений
- `redis.py` — клиент для Redis
- `requirements.txt` — зависимости Python
- `dockerfile.dag` — Dockerfile для сборки образа
- `.env.dev`, `.env.prod` — примеры переменных окружения
- `deploy.sh` — скрипт деплоя в Kubernetes

## Пример запуска
```bash
docker run --env-file .env.prod find_outliers:latest --email user@example.com
```

## Пример входных данных

В качестве входных данных используется email пользователя, зарегистрированного в системе. Все необходимые данные (сырые и обработанные записи) агрегируются автоматически из баз данных.
