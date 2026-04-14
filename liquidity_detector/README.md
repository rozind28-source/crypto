# Liquidity Detector - Crypto Anomaly Detection System

MVP система для детекции аномалий ликвидности на криптовалютных биржах в реальном времени.

## 🎯 Возможности

- **Volume Delta Divergence**: Обнаружение расхождений между объемом торгов и движением цены
- **Spoofing/Layering Detection**: Детекция фейковых ордеров в стакане
- **Real-time анализ**: Подключение к WebSocket потокам Bybit v5
- **Telegram алерты**: Мгновенные уведомления с деталями аномалий

## 📁 Структура проекта

```
liquidity_detector/
├── config.py          # Pydantic-модели конфигурации
├── ws_client.py       # Асинхронный WS-клиент для Bybit
├── normalizer.py      # Парсинг и валидация данных
├── detector.py        # Ядро детекции аномалий
├── alerter.py         # Telegram-отправка уведомлений
├── main.py            # Точка входа, orchestrator
├── requirements.txt   # Зависимости Python
├── .env.example       # Шаблон конфигурации
└── logs/              # Директория для логов
```

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка конфигурации

```bash
cp .env.example .env
```

Отредактируйте `.env` и укажите:
- `TELEGRAM_BOT_TOKEN` - токен бота от @BotFather
- `TELEGRAM_CHAT_ID` - ID чата для получения алертов

### 3. Запуск

```bash
python main.py
```

## 🔧 Конфигурация детектора

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `DELTA_WINDOW_SEC` | 60 | Окно для расчета дельты (сек) |
| `DELTA_THRESHOLD_USDT` | 50000 | Порог дельты для алерта ($) |
| `PRICE_CHANGE_THRESHOLD_PCT` | 0.5 | Макс. изменение цены (%) |
| `SPOOF_SIZE_THRESHOLD_USDT` | 100000 | Мин. размер ордера для мониторинга ($) |
| `SPOOF_LIFETIME_SEC` | 0.5 | Время жизни спуф-ордера (сек) |

## 📊 Формат алертов

### Volume Delta Divergence
```
📊 ЛИКВИДНОСТЬ: BTCUSDT
🔍 Аномалия: DIVERGENCE
• Delta Usdt: $150,000.00
• Price Change Pct: 0.002%
• Direction: BULLISH
⏱ Время: 2024-01-15 14:30:00 UTC
🔗 Открыть график
```

### Spoofing Detection
```
🎭 ЛИКВИДНОСТЬ: BTCUSDT
🔍 Аномалия: SPOOFING
• Price: $42,000.00
• Side: Buy
• Size Usdt: $250,000.00
• Lifetime Sec: 0.3s
⏱ Время: 2024-01-15 14:30:00 UTC
🔗 Открыть график
```

## 🛠 Архитектура

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│   Bybit WS  │────▶│  Normalizer  │────▶│   Detector  │────▶│   Alerter   │
│   Client    │     │              │     │             │     │  (Telegram) │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
                           │                    │
                      Queue               Deque (window)
                                         Dict (state)
```

## 📈 Рекомендации по масштабированию

### Для production использования:

1. **Redis** - для распределенного хранения состояния order book
2. **TimescaleDB/ClickHouse** - для хранения исторических данных и backtesting
3. **Kafka/RabbitMQ** - для очередей сообщений между микросервисами
4. **ML модели** - для улучшения детекции аномалий (Isolation Forest, Autoencoders)
5. **Docker/Kubernetes** - для контейнеризации и оркестрации
6. **Prometheus/Grafana** - для мониторинга метрик системы

## ⚠️ Отказ от ответственности

Система предназначена для образовательных и исследовательских целей. 
Не является финансовым советом. Используйте на свой риск.

## 📄 Лицензия

MIT License
