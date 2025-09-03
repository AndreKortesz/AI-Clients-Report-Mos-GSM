import os
from fastapi import FastAPI, Request
from apscheduler.schedulers.background import BackgroundScheduler
from logic import detect_alerts
from telegram_bot import send_message, format_alerts

PORT = int(os.getenv("PORT", "8000"))
CRON_MINUTES = int(os.getenv("CRON_MINUTES", "60"))  # периодичность скана

app = FastAPI(title="Bitrix Alerts")

scheduler = BackgroundScheduler(timezone="UTC")
def job_scan():
    try:
        alerts = detect_alerts()
        if alerts:
            send_message(format_alerts(alerts))
    except Exception as e:
        send_message(f"❗️Ошибка скана: {e}")

scheduler.add_job(job_scan, "interval", minutes=CRON_MINUTES, id="scan")
scheduler.start()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run-scan")
async def run_scan():
    alerts = detect_alerts()
    txt = format_alerts(alerts)
    send_message(txt)
    return {"alerts": alerts, "sent": True}

# Опционально: приём событий Битрикса (подключишь позже)
@app.post("/bitrix/events")
async def bitrix_events(request: Request):
    data = await request.json()
    # Здесь можно ловить onCrmActivityAdd / onTelephonyCallEnd и запускать узкий анализ.
    # Пока просто подтверждаем.
    return {"result": "ok", "echo": data}
