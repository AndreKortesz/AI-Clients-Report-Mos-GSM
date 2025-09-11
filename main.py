# main.py — ежедневный запуск в 19:00, с сохранением всех debug-эндпоинтов

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Query
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from logic import detect_alerts
from telegram_bot import send_message, format_alerts
from bitrix import list_activities

# === Настройки планировщика ===
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow")
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "19"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))
TZ = ZoneInfo(TZ_NAME)

# (устарело) Переменная CRON_MINUTES больше не используется
if os.getenv("CRON_MINUTES"):
    print("[WARN] CRON_MINUTES больше не используется. Расписание задаётся через SCHEDULE_HOUR/SCHEDULE_MINUTE.")

app = FastAPI(title="Bitrix Alerts")

# === Планировщик: один раз в день ===
scheduler = AsyncIOScheduler(timezone=TZ)

def job_scan():
    """Ежедневная задача: собрать тревоги и отправить отчёт в Telegram."""
    try:
        alerts = detect_alerts()
        text = format_alerts(alerts)  # твоя функция форматирования
        # Отправляем ежедневный дайджест всегда (и когда пусто — придёт 'На сейчас тревог нет.')
        send_message(text)
    except Exception as e:
        send_message(f"❗️Ошибка скана: {e}")

@app.on_event("startup")
def _on_startup():
    # Чистим возможные дубликаты (на случай горячего рестарта)
    for job in scheduler.get_jobs():
        scheduler.remove_job(job.id)

    trigger = CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE)
    scheduler.add_job(job_scan, trigger, id="daily_scan")
    scheduler.start()
    print(f"[SCHEDULER] План: каждый день в {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} ({TZ_NAME})")

@app.on_event("shutdown")
def _on_shutdown():
    scheduler.shutdown(wait=False)

# === Служебные эндпоинты ===

@app.get("/health")
def health():
    now = datetime.now(TZ).isoformat()
    return {"ok": True, "time": now, "timezone": TZ_NAME}

@app.post("/run-scan")
async def run_scan():
    """Ручной запуск из Swagger/curl."""
    try:
        alerts = detect_alerts()
        txt = format_alerts(alerts)
        send_message(txt)
        return {"alerts": alerts, "sent": True}
    except Exception as e:
        send_message(f"❗️Ошибка скана: {e}")
        return {"error": str(e)}

# Приём событий Битрикса (можно расширить позже)
@app.post("/bitrix/events")
async def bitrix_events(request: Request):
    data = await request.json()
    return {"result": "ok", "echo": data}

# === Debug утилиты (сохранены как у тебя) ===

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
    """
    from collections import Counter

    since = _iso(datetime.now(timezone.utc) - timedelta(days=days))
    rows = list_activities(
        {"DIRECTION": 2, ">=CREATED": since},
        order={"CREATED": "DESC"},
        select=["PROVIDER_ID","PROVIDER_TYPE_ID"]
    )
    rows = rows[:limit]

    by_provider = Counter((r.get("PROVIDER_ID") or "").upper() for r in rows)
    by_type = Counter((r.get("PROVIDER_TYPE_ID") or "").upper() for r in rows)

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
