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

from datetime import datetime, timedelta, timezone
from collections import Counter
from fastapi import Query
from bitrix import list_activities

def _iso(dt): 
    return dt.astimezone(timezone.utc).isoformat()

@app.get("/debug/last-incomings")
def debug_last_incomings(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Последние входящие активности (DIRECTION=2) за N дней.
    Вернём ID, CREATED, PROVIDER_ID, PROVIDER_TYPE_ID, OWNER_TYPE/ID.
    """
    since = _iso(datetime.now(timezone.utc) - timedelta(days=days))
    rows = list_activities(
        {"DIRECTION": 2, ">=CREATED": since},
        order={"CREATED": "DESC"},
        select=[
            "ID","CREATED","PROVIDER_ID","PROVIDER_TYPE_ID","DIRECTION",
            "OWNER_TYPE_ID","OWNER_ID","COMMUNICATIONS","AUTHOR_ID","SUBJECT"
        ]
    )
    return rows[:limit]

@app.get("/debug/providers-summary")
def debug_providers_summary(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(2000, ge=100, le=10000),
):
    """
    Сводка по каналам за N дней: сколько входящих по каждому PROVIDER_ID/TYPE.
    Увеличивай limit, если мало записей.
    """
    since = _iso(datetime.now(timezone.utc) - timedelta(days=days))
    rows = list_activities(
        {"DIRECTION": 2, ">=CREATED": since},
        order={"CREATED": "DESC"},
        select=["PROVIDER_ID","PROVIDER_TYPE_ID"]
    )
    rows = rows[:limit]

    by_provider = Counter((r.get("PROVIDER_ID") or "").upper() for r in rows)
    by_type = Counter((r.get("PROVIDER_TYPE_ID") or "").upper() for r in rows)

    # Сводка в виде удобного JSON
    return {
        "total_sampled": len(rows),
        "by_PROVIDER_ID": [
            {"PROVIDER_ID": k or "(empty)", "count": v}
            for k, v in by_provider.most_common()
        ],
        "by_PROVIDER_TYPE_ID": [
            {"PROVIDER_TYPE_ID": k or "(empty)", "count": v}
            for k, v in by_type.most_common()
        ]
    }

@app.get("/debug/activities-by-entity")
def debug_activities_by_entity(
    owner_type_id: int = Query(..., description="1=Лид, 2=Контакт, 3=Компания, 4=Сделка"),
    owner_id: int = Query(...),
    days: int = Query(60, ge=1, le=365),
    limit: int = Query(200, ge=1, le=1000),
):
    """
    Все активности по конкретной сущности за N дней — удобно смотреть конкретный кейс.
    """
    since = _iso(datetime.now(timezone.utc) - timedelta(days=days))
    rows = list_activities(
        {
            "OWNER_TYPE_ID": owner_type_id,
            "OWNER_ID": owner_id,
            ">=CREATED": since,
        },
        order={"CREATED": "DESC"},
        select=[
            "ID","CREATED","TYPE_ID","PROVIDER_ID","PROVIDER_TYPE_ID","DIRECTION",
            "SUBJECT","COMPLETED","AUTHOR_ID"
        ]
    )
    return rows[:limit]
