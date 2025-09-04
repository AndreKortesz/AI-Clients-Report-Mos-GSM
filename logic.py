from datetime import datetime, timedelta, timezone
import os

from bitrix import list_activities, list_calls_since

# === Настройки ===
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "14"))
RESPONSE_SLA_MIN = int(os.getenv("RESPONSE_SLA_MIN", "45"))
MAX_ROWS_INCOMING = int(os.getenv("MAX_ROWS_INCOMING", "1500"))   # сколько входящих максимум забираем за раз
MAX_ROWS_REPLY    = int(os.getenv("MAX_ROWS_REPLY", "300"))       # проверка исходящих после входящего
MAX_ROWS_CALL_ACT = int(os.getenv("MAX_ROWS_CALL_ACT", "200"))    # проверка звонковых активностей

# Каналы-провайдеры, которые считаем "перепиской" (по PROVIDER_ID)
PROVIDERS_MSG = {
    (p or "").strip().upper()
    for p in (os.getenv("PROVIDERS_MSG") or "IMOPENLINES_SESSION,CRM_EMAIL,WAZZUP").split(",")
    if (p or "").strip()
}

# (Опц.) Каналы-подтипы (по PROVIDER_TYPE_ID), напр. WHATSAPP/EMAIL/15
PROVIDERS_TYPE = {
    str((p or "").strip()).upper()
    for p in (os.getenv("PROVIDERS_TYPE") or "WHATSAPP,EMAIL,15").split(",")
    if str((p or "").strip())
}

def _as_upper(v) -> str:
    """Безопасно привести к строке и upper()."""
    return str(v if v is not None else "").strip().upper()

def _is_message_activity(row: dict) -> bool:
    """True, если активность относится к переписке по нашим правилам."""
    prov  = _as_upper(row.get("PROVIDER_ID"))
    ptype = _as_upper(row.get("PROVIDER_TYPE_ID"))
    ok_by_id = prov in PROVIDERS_MSG if prov else False
    ok_by_type = (len(PROVIDERS_TYPE) > 0 and ptype in PROVIDERS_TYPE) if ptype else False
    return ok_by_id or ok_by_type

# Типы сущностей в Bitrix: 1-Лид, 2-Контакт, 3-Компания, 4-Сделка
TRACK_ENTITY_TYPES = {
    s.strip() for s in (os.getenv("ENTITY_TYPES") or "1,2,3,4").split(",") if s.strip()
}

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def _parse_b24_iso(s: str) -> datetime:
    """Парсит дату из Bitrix (с Z или с оффсетом)."""
    if not s:
        return datetime.now(timezone.utc)
    s = str(s)
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)

# === Поиск последних входящих сообщений ===
def fetch_recent_incoming_messages():
    since = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    flt = {
        ">=CREATED": _iso(since),
        "DIRECTION": 2,  # incoming от клиента
    }
    # ВАЖНО: включаем PROVIDER_TYPE_ID — нужен для фильтра по типам (WHATSAPP и т.д.)
    select = [
        "ID","CREATED","PROVIDER_ID","PROVIDER_TYPE_ID","SUBJECT",
        "OWNER_TYPE_ID","OWNER_ID","COMMUNICATIONS","AUTHOR_ID","DESCRIPTION"
    ]
    rows = list_activities(
        flt,
        order={"CREATED": "DESC"},
        select=select,
        max_rows=MAX_ROWS_INCOMING
    )

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
    # Берём PROVIDER_TYPE_ID, чтобы учесть фильтр по типам
    select = ["ID","CREATED","PROVIDER_ID","PROVIDER_TYPE_ID","AUTHOR_ID"]
    rows = list_activities(
        flt,
        order={"CREATED": "ASC"},
        select=select,
        max_rows=MAX_ROWS_REPLY
    )
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
        select=["ID","CREATED","PROVIDER_ID","DIRECTION","COMPLETED","SETTINGS"],
        max_rows=MAX_ROWS_CALL_ACT
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
            return str(v)
    return None

# === Главный детектор тревог ===
def detect_alerts():
    """
    Возвращает список словарей:
    {
      'owner_type_id', 'owner_id', 'last_in_created',
      'provider_id', 'phone', 'activity_id', 'subject'
    }
    """
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
        # Парсим дату
        created_raw = str(last.get("CREATED"))
        t_in = _parse_b24_iso(created_raw)

        # Ждём SLA
        if (now_utc - t_in).total_seconds() < RESPONSE_SLA_MIN * 60:
            continue

        # Был ли исходящий ответ после входящего
        if has_outgoing_reply_after(etype, eid, _iso(t_in)):
            continue

        # Был ли звонок после входящего
        phone = communications_first_phone(last.get("COMMUNICATIONS"))
        if has_success_call_after(etype, eid, _iso(t_in), phone):
            continue

        alerts.append({
            "owner_type_id": etype,
            "owner_id": eid,
            "last_in_created": created_raw,
            "provider_id": last.get("PROVIDER_ID"),
            "phone": phone,
            "activity_id": last.get("ID"),
            "subject": last.get("SUBJECT") or "",
        })

    return alerts
