# Используем многоэтапную сборку
# Этап сборки
FROM python:3.9-slim as builder

WORKDIR /

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создаем виртуальное окружение
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ---
# Финальный этап
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    dnsutils \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*
    
# Копируем виртуальное окружение
COPY --from=builder /opt/venv /opt/venv

# Копируем только нужные файлы
COPY ./app .

# Устанавливаем переменные окружения
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLASK_ENV=production

# Создаем пользователя
RUN groupadd -r appuser && useradd -r -g appuser appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 9443

HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:9443/healthz || exit 1

#команда запуска
CMD ["gunicorn", "--preload", "--bind", "0.0.0.0:9443", "--workers", "2", "--threads", "1", "--timeout", "120", "app:app"]