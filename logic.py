from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os

from bitrix import list_activities, list_calls_since

# Настройки
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "14"))
RESPONSE_SLA_MIN = int(os.getenv("RESPONSE_SLA_MIN", "45"))

# Каналы-провайдеры, которые считаем "перепиской"
PROVIDERS_MSG = {
    p.strip().upper()
    for p in (os.getenv("PROVIDERS_MSG") or "IMOPENLINES,OPENLINE,WHATSAPP,TELEGRAMBOT,EMAIL").split(",")
    if p.strip()
}

PROVIDERS_TYPE = {
    p.strip().upper()
    for p in (os.getenv("PROVIDERS_TYPE") or "").split(",")
    if p.strip()
}

def _is_message_activity(row):
    prov = (row.get("PROVIDER_ID") or "").upper()
    ptype = (row.get("PROVIDER_TYPE_ID") or "").upper()
    ok_by_id = prov in PROVIDERS_MSG
    ok_by_type = (PROVIDERS_TYPE and ptype in PROVIDERS_TYPE)
    return ok_by_id or ok_by_type

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
        and _is_message_activity(r)]

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

from bitrix import list_activities, list_calls_since

def has_success_call_after(entity_type_id, entity_id, t_from_iso, phone):
    # 1) Сначала проверим журнал телефонии (быстрее и надёжнее)
    calls = list_calls_since(t_from_iso,
                             entity_type_id=int(entity_type_id),
                             entity_id=int(entity_id),
                             phone=phone)
    for c in calls:
        if str(c.get("CALL_FAILED","N")).upper() != "Y":
            return True  # успешный входящий или исходящий после входящего сообщения

    # 2) Фоллбек: проверим CRM-активности звонка (VOXIMPLANT_CALL / CALL)
    rows = list_activities(
        {
            "OWNER_TYPE_ID": int(entity_type_id),
            "OWNER_ID": int(entity_id),
            ">CREATED": t_from_iso,
            "PROVIDER_ID": ["VOXIMPLANT_CALL", "CALL"]
        },
        order={"CREATED": "ASC"},
        select=["ID","CREATED","PROVIDER_ID","DIRECTION","COMPLETED","SETTINGS"]
    )
    for r in rows:
        # считаем любой завершённый звонок достаточным
        if (r.get("COMPLETED") == "Y") or (str(r.get("DIRECTION","0")) in ("1","2")):
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
