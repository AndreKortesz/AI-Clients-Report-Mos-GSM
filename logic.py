from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os

from bitrix import list_activities, list_calls_since

# Настройки
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "14"))
RESPONSE_SLA_MIN = int(os.getenv("RESPONSE_SLA_MIN", "45"))

# Каналы-провайдеры, которые считаем "перепиской"
PROVIDERS_MSG = set((os.getenv("PROVIDERS_MSG") or
    "IMOPENLINES,OPENLINE,WHATSAPP,TELEGRAMBOT,EMAIL")).split(","))

# Типы сущностей в Bitrix
# 1 - Лид, 2 - Контакт, 3 - Компания, 4 - Сделка
TRACK_ENTITY_TYPES = set((os.getenv("ENTITY_TYPES") or "1,2,3,4").split(","))

def _iso(dt): return dt.astimezone(timezone.utc).isoformat()

def fetch_recent_incoming_messages():
    since = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    flt = {
        ">=CREATED": _iso(since),
        "DIRECTION": 2,  # incoming
        # фильтр по провайдерам сделаем ниже в коде
    }
    select = ["ID","CREATED","PROVIDER_ID","SUBJECT","OWNER_TYPE_ID","OWNER_ID","COMMUNICATIONS","AUTHOR_ID","DESCRIPTION"]
    rows = list_activities(flt, order={"CREATED":"DESC"}, select=select)
    # Отфильтруем только нужные провайдеры и сущности
    return [r for r in rows
            if str(r.get("OWNER_TYPE_ID")) in TRACK_ENTITY_TYPES
            and (r.get("PROVIDER_ID") or "").upper() in PROVIDERS_MSG]

def has_outgoing_reply_after(entity_type_id, entity_id, t_from_iso):
    flt = {
        "OWNER_TYPE_ID": int(entity_type_id),
        "OWNER_ID": int(entity_id),
        ">CREATED": t_from_iso,
        "DIRECTION": 1,  # outgoing
    }
    select = ["ID","CREATED","PROVIDER_ID","AUTHOR_ID"]
    rows = list_activities(flt, order={"CREATED":"ASC"}, select=select)
    for r in rows:
        prov = (r.get("PROVIDER_ID") or "").upper()
        if prov in PROVIDERS_MSG:
            return True
    return False

def has_success_call_after(entity_type_id, entity_id, t_from_iso, phone):
    calls = list_calls_since(t_from_iso, entity_type_id=int(entity_type_id), entity_id=int(entity_id), phone=phone)
    for c in calls:
        # CALL_TYPE: 1 inbound, 2 outbound
        # CALL_FAILED: 'Y'/'N'
        if str(c.get("CALL_FAILED","N")).upper() == "Y":
            continue
        # Успешный входящий или исходящий — считаем «погасившим» тревогу
        return True
    return False

def communications_first_phone(comms):
    if isinstance(comms, list) and comms:
        v = comms[0].get("VALUE")
        if v: return v
    return None

def detect_alerts():
    """Возвращает список тревог вида:
       { 'owner_type_id', 'owner_id', 'last_in_created', 'provider_id', 'phone', 'activity_id' }"""
    incomings = fetch_recent_incoming_messages()
    alerts = []

    # берём только ПОСЛЕДНЕЕ входящее по каждой сущности
    latest_by_entity = {}
    for r in incomings:
        key = (str(r["OWNER_TYPE_ID"]), str(r["OWNER_ID"]))
        if key not in latest_by_entity:
            latest_by_entity[key] = r  # уже отсортировано DESC

    now_utc = datetime.now(timezone.utc)

    for (etype, eid), last in latest_by_entity.items():
        t_in = datetime.fromisoformat(last["CREATED"].replace("Z","+00:00"))
        # ждём SLA
        if (now_utc - t_in).total_seconds() < RESPONSE_SLA_MIN * 60:
            continue

        # проверяем исходящий ответ после входящего
        if has_outgoing_reply_after(etype, eid, _iso(t_in)):
            continue

        # проверяем звонок после входящего
        phone = communications_first_phone(last.get("COMMUNICATIONS"))
        if has_success_call_after(etype, eid, _iso(t_in), phone):
            continue

        alerts.append({
            "owner_type_id": etype,
            "owner_id": eid,
            "last_in_created": last["CREATED"],
            "provider_id": last.get("PROVIDER_ID"),
            "phone": phone,
            "activity_id": last.get("ID"),
            "subject": last.get("SUBJECT") or "",
        })

    return alerts
