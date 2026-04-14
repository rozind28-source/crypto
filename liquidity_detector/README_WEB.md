# 📊 Liquidity Anomaly Detector (Web UI Version)

Система детектирования аномалий ликвидности для криптовалютных бирж с **веб-интерфейсом реального времени**.

## 🚀 Что нового

Версия с **веб-интерфейсом** вместо Telegram-алертов:
- ✅ Красивый дашборд с метриками в реальном времени
- ✅ Выбор монеты из списка популярных символов
- ✅ WebSocket для мгновенных обновлений
- ✅ История алертов с фильтрацией по типу
- ✅ Не требует настройки Telegram

## 🏗 Архитектура потока данных

```
Bybit WebSocket → WS Client → Normalizer → Detector → Web API → Browser
     │                                              │
     └────────── Order Book & Trades ───────────────┘
```

1. **WS Client** подключается к Bybit v5 WebSocket
2. **Normalizer** парсит и валидирует данные
3. **Detector** анализирует аномалии:
   - Volume Delta Divergence
   - Spoofing/Layering
4. **Web API** (FastAPI) отправляет алерты в браузер через WebSocket
5. **Frontend** отображает данные в реальном времени

## 📁 Структура проекта

```
liquidity_detector/
├── .env.example          # Шаблон конфигурации
├── config.py             # Pydantic-модели конфига
├── ws_client.py          # Асинхронный WS-клиент для Bybit
├── normalizer.py         # Парсинг и валидация данных
├── detector.py           # Ядро детекции аномалий
├── web_api.py            # FastAPI сервер + HTML интерфейс
├── main.py               # Точка входа, orchestrator
├── requirements.txt      # Зависимости Python
└── logs/                 # Логи приложения
```

## 🛠 Установка

```bash
# Клонировать или перейти в директорию
cd liquidity_detector

# Установить зависимости
pip install -r requirements.txt

# (Опционально) Скопировать конфиг
cp .env.example .env
```

## 🚀 Запуск

### Базовый запуск (BTCUSDT)
```bash
python main.py
```

### Запуск с выбором монеты
```bash
python main.py --symbol=ETHUSDT
python main.py --symbol=SOLUSDT
```

### После запуска
Откройте в браузере: **http://localhost:8000**

## 🎯 Детектируемые аномалии

### 1. Volume Delta Divergence
Ситуация, когда большой объем сделок не двигает цену.

**Формула:**
```
Delta = Σ(buy_volume × price) - Σ(sell_volume × price)
```

**Условия срабатывания:**
- `|price_change| < 0.5%` (цена почти не изменилась)
- `|delta| > $50,000` (большой дисбаланс объема)

**Интерпретация:**
- 🟢 **BULLISH**: Большая покупка, но цена не растет → кто-то поглощает продажи
- 🔴 **BEARISH**: Большая продажа, но цена не падает → кто-то поглощает покупки

### 2. Spoofing / Layering
Фейковые ордера, которые появляются и быстро исчезают.

**Условия срабатывания:**
- Ордер > $100,000 USDT
- Исчез менее чем через 0.5 секунды

**Интерпретация:**
- Попытка манипуляции стаканом для создания ложного впечатления о спросе/предложении

## ⚙️ Настройка порогов

В `.env` файле можно настроить:

```ini
# Volume Delta Divergence
DELTA_WINDOW_SEC=60              # Окно анализа (сек)
DELTA_THRESHOLD_USDT=50000       # Мин. дельта для алерта
PRICE_CHANGE_THRESHOLD_PCT=0.5   # Макс. изменение цены (%)

# Spoofing Detection
SPOOF_SIZE_THRESHOLD_USDT=100000 # Мин. размер ордера ($)
SPOOF_LIFETIME_SEC=0.5           # Макс. время жизни ордера (сек)

# Web Server
WEB_HOST=0.0.0.0
WEB_PORT=8000
```

## 🌐 Веб-интерфейс

### Элементы дашборда:

1. **Market Data Card**
   - Best Bid / Best Ask
   - Mid Price
   - Last Trade
   - Volume Delta (цветом: зеленый + / красный -)
   - Trades in Window
   - Tracked Orders

2. **Detection Settings Card**
   - Текущие пороги детекции

3. **Real-time Alerts**
   - Лента алертов в реальном времени
   - Цветовая кодировка:
     - 🟠 **DIVERGENCE** - оранжевый
     - 🔴 **SPOOFING** - красный
   - Детали: delta, price change, lifetime и т.д.

4. **Symbol Selector**
   - Выпадающий список популярных монет
   - Кнопка "Change Symbol" для переключения

### Доступные символы:
BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, BNBUSDT, DOGEUSDT, ADAUSDT, AVAXUSDT, TRXUSDT, LINKUSDT, MATICUSDT, DOTUSDT, LTCUSDT, ATOMUSDT, UNIUSDT

## 📡 API Endpoints

### REST API
- `GET /` - Веб-интерфейс
- `GET /api/market` - Текущие рыночные данные
- `GET /api/alerts?limit=50` - История алертов
- `GET /api/symbols` - Список доступных символов

### WebSocket
- `WS /ws` - Стриминг алертов и market data в реальном времени

## 🔧 Разработка

### Добавление нового символа
Просто укажите его в командной строке:
```bash
python main.py --symbol=YOURSYMBOL
```

### Изменение логики детекции
Правьте `detector.py`:
- `_check_divergence()` - логика дивергенции
- `_check_spoofing()` - логика спуфинга

### Модификация интерфейса
HTML/CSS/JS находятся в `web_api.py` в методе `_get_inline_html()`.

## 📈 Масштабирование (рекомендации)

Для production-использования:

1. **Redis** - для shared state между инстансами
2. **TimescaleDB/ClickHouse** - для хранения истории алертов
3. **ML-модели** - для классификации паттернов
4. **Kubernetes** - для оркестрации множества пар
5. **Rate Limiting** - для защиты API

## ⚠️ Отказ от ответственности

Система предоставляет информацию для образовательных целей. Не является финансовым советом. Используйте на свой риск.

## 📄 Лицензия

MIT License
