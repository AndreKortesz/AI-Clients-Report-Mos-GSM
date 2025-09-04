# Bitrix Alerts (Railway)

## Переменные окружения (Railway → Variables)
B24_WEBHOOK="https://svyaz.bitrix24.ru/rest/9/g6f763i7yl75xgel/"
TELEGRAM_TOKEN="7356362291:AAEJPWf44b_2F0LoEYLpvoZ6NEDKum4bdTs"
TELEGRAM_CHAT_ID="260027381"
WINDOW_DAYS="3"
RESPONSE_SLA_MIN="0"
CRON_MINUTES="60"
PROVIDERS_MSG="IMOPENLINES_SESSION,CRM_EMAIL,WAZZUP"
PROVIDERS_TYPE="WHATSAPP,EMAIL,15"
ENTITY_TYPES="1,2,3,4"
MAX_ROWS_INCOMING="1500"
MAX_ROWS_REPLY="300"
MAX_ROWS_CALL_ACT="200"

## Локально
uvicorn main:app --host 0.0.0.0 --port 8000

## Railway
- Подключи репозиторий из GitHub.
- Buildpack: Nixpacks (по умолчанию).
- Start Command: uvicorn main:app --host 0.0.0.0 --port $PORT
- Задай переменные окружения.
