# script1/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создание директории для логов
RUN mkdir -p /app/data

# Копирование исходного кода
COPY . .

# Запуск скрипта при старте контейнера
CMD ["python", "-m", "app.main"]