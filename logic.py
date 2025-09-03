from datetime import datetime, timedelta, timezone
import os

from bitrix import list_activities, list_calls_since

# === Настройки ===
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "14"))
RESPONSE_SLA_MIN = int(os.getenv("RESPONSE_SLA_MIN", "45"))

# Каналы-провайдеры, которые считаем "перепиской" (по PROVIDER_ID)
PROVIDERS_MSG = {
    p.strip().upper()
    for p in (os.getenv("PROVIDERS_MSG") or "IMOPENLINES,OPENLINE,WHATSAPP,TELEGRAMBOT,EMAIL").split(",")
    if p.strip()
}

# (Опц.) Каналы-подтипы (по PROVIDER_TYPE_ID), напр. WHATSAPP/TELEGRAM, если всё идёт через IMOPENLINES
PROVIDERS_TYPE = {
    p.strip().upper()
    for p in (os.getenv("PROVIDERS_TYPE") or "").split(",")
    if p.strip()
}

def _is_message_activity(row: dict) -> bool:
    """Возвращает True, если активность относится к переписке по нашим правилам."""
    prov = (row.get("PROVIDER_ID") or "").upper()
    ptype = (row.get("PROVIDER_TYPE_ID") or "").upper()
    ok_by_id = prov in PROVIDERS_MSG
    ok_by_type = (len(PROVIDERS_TYPE) > 0 and ptype in PROVIDERS_TYPE)
    return ok_by_id or ok_by_type

# Типы сущностей в Bitrix: 1-Лид, 2-Контакт, 3-Компания, 4-Сделка
TRACK_ENTITY_TYPES = {
    s.strip() for s in (os.getenv("ENTITY_TYPES") or "1,2,3,4").split(",") if s.strip()
}

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

# === Поиск последних входящих сообщений ===
def fetch_recent_incoming_messages():
    since = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    flt = {
        ">=CREATED": _iso(since),
        "DIRECTION": 2,  # incoming от клиента
    }
    # ВАЖНО: включаем PROVIDER_TYPE_ID — он нужен для фильтра по типам (WHATSAPP и т.д.)
    select = [
        "ID","CREATED","PROVIDER_ID","PROVIDER_TYPE_ID","SUBJECT",
        "OWNER_TYPE_ID","OWNER_ID","COMMUNICATIONS","AUTHOR_ID","DESCRIPTION"
    ]
    rows = list_activities(flt, order={"CREATED": "DESC"}, select=select)

    # Оставляем только нужные сущности и только переписку
    return [
        r for r in rows
        if str(r.get("OWNER_TYPE_ID")) in TRACK_ENTITY_TYPES and _is_message_activity(r)
    ]

# === Был ли исходящий ответ после входящего ===
def has_outgoing_reply_after(entity_type_id, entity_id, t_from_iso: str) -> bool:
    flt = {
        "OWNER_TYPE_ID": int(entity_type_id),
        "OWNER_ID": int(entity_id),
        ">CREATED": t_from_iso,
        "DIRECTION": 1,  # outgoing от менеджера
    }
    # Тоже берём PROVIDER_TYPE_ID, чтобы учесть фильтр по типам
    select = ["ID","CREATED","PROVIDER_ID","PROVIDER_TYPE_ID","AUTHOR_ID"]
    rows = list_activities(flt, order={"CREATED": "ASC"}, select=select)
    for r in rows:
        if _is_message_activity(r):
            return True
    return False

# === Был ли звонок после входящего ===
def has_success_call_after(entity_type_id, entity_id, t_from_iso: str, phone: str | None) -> bool:
    # 1) Журнал телефонии (надёжнее и быстрее)
    calls = list_calls_since(
        t_from_iso,
        entity_type_id=int(entity_type_id),
        entity_id=int(entity_id),
        phone=phone
    )
    for c in calls:
        # успешный входящий/исходящий
        if str(c.get("CALL_FAILED", "N")).upper() != "Y":
            return True

    # 2) Фоллбек: активности звонков в CRM (например, VOXIMPLANT_CALL / CALL)
    rows = list_activities(
        {
            "OWNER_TYPE_ID": int(entity_type_id),
            "OWNER_ID": int(entity_id),
            ">CREATED": t_from_iso,
            "PROVIDER_ID": ["VOXIMPLANT_CALL", "CALL"],
        },
        order={"CREATED": "ASC"},
        select=["ID","CREATED","PROVIDER_ID","DIRECTION","COMPLETED","SETTINGS"]
    )
    for r in rows:
        # Любой завершённый звонок или наличие направления 1/2 считаем достаточным
        if r.get("COMPLETED") == "Y" or str(r.get("DIRECTION", "0")) in ("1", "2"):
            return True

    return False

def communications_first_phone(comms):
    if isinstance(comms, list) and comms:
        v = comms[0].get("VALUE")
        if v:
            return v
    return None

# === Главный детектор тревог ===
def detect_alerts():
    """Возвращает список:
       { 'owner_type_id', 'owner_id', 'last_in_created', 'provider_id', 'phone', 'activity_id', 'subject' }"""
    incomings = fetch_recent_incoming_messages()
    alerts = []

    # Берём только ПОСЛЕДНЕЕ входящее по каждой сущности
    latest_by_entity: dict[tuple[str, str], dict] = {}
    for r in incomings:
        key = (str(r["OWNER_TYPE_ID"]), str(r["OWNER_ID"]))
        if key not in latest_by_entity:
            latest_by_entity[key] = r  # уже отсортировано DESC

    now_utc = datetime.now(timezone.utc)

    for (etype, eid), last in latest_by_entity.items():
        # Строки
