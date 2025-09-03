# Bitrix Alerts (Railway)

## Переменные окружения (Railway → Variables)
B24_WEBHOOK="https://svyaz.bitrix24.ru/rest/9/g6f763i7yl75xgel/"
TELEGRAM_TOKEN="7356362291:AAEJPWf44b_2F0LoEYLpvoZ6NEDKum4bdTs"
TELEGRAM_CHAT_ID="-1007356362291"
WINDOW_DAYS="14"
RESPONSE_SLA_MIN="60"
CRON_MINUTES="60"
PROVIDERS_MSG="IMOPENLINES,OPENLINE,WHATSAPP,TELEGRAMBOT,EMAIL"
ENTITY_TYPES="1,2,3,4"

## Локально
uvicorn main:app --host 0.0.0.0 --port 8000

## Railway
- Подключи репозиторий из GitHub.
- Buildpack: Nixpacks (по умолчанию).
- Start Command: uvicorn main:app --host 0.0.0.0 --port $PORT
- Задай переменные окружения.
