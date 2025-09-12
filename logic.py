# logic.py
from datetime import datetime, timedelta, timezone
import os

from bitrix import list_activities, list_calls_since, get_last_openlines_messages

# === Настройки ===
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "14"))
RESPONSE_SLA_MIN = int(os.getenv("RESPONSE_SLA_MIN", "45"))
MAX_ROWS_INCOMING = int(os.getenv("MAX_ROWS_INCOMING", "1500"))
MAX_ROWS_REPLY    = int(os.getenv("MAX_ROWS_REPLY", "300"))
MAX_ROWS_CALL_ACT = int(os.getenv("MAX_ROWS_CALL_ACT", "200"))

# Каналы-провайдеры, которые считаем "перепиской"
PROVIDERS_MSG = {
    (p or "").strip().upper()
    for p in (os.getenv("PROVIDERS_MSG") or "IMOPENLINES_SESSION,CRM_EMAIL,WAZZUP").split(",")
    if (p or "").strip()
}

# Подтипы каналов
PROVIDERS_TYPE = {
    str((p or "").strip()).upper()
    for p in (os.getenv("PROVIDERS_TYPE") or "WHATSAPP,EMAIL,15").split(",")
    if str((p or "").strip())
}

# Какие provider считаем "сеансовыми" (не сообщение, а сессия ОЛ)
OPENLINES_SESSION_IDS = {"IMOPENLINES_SESSION"}
OPENLINES_SESSION_TYPES = {"15"}  # тип канала Wazzup в ОЛ в твоих данных

def _as_upper(v) -> str:
    return str(v if v is not None else "").strip().upper()

def _is_message_activity(row: dict) -> bool:
    prov  = _as_upper(row.get("PROVIDER_ID"))
    ptype = _as_upper(row.get("PROVIDER_TYPE_ID"))
    ok_by_id = prov in PROVIDERS_MSG if prov else False
    ok_by_type = (len(PROVIDERS_TYPE) > 0 and ptype in PROVIDERS_TYPE) if ptype else False
    return ok_by_id or ok_by_type

# Типы сущностей
TRACK_ENTITY_TYPES = {
    s.strip() for s in (os.getenv("ENTITY_TYPES") or "1,2,3,4").split(",") if s.strip()
}

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def _parse_b24_iso(s: str) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    s = str(s)
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)

def _extract_dialog_id_from_comms(comms) -> str | None:
    """
    COMMUNICATIONS[].VALUE для ОЛ выглядит как:
    'imol|wz_whatsapp_...|15|53a528ba-...|35855'
    Нам нужен этот VALUE целиком как DIALOG_ID.
    Берём первый TYPE='IM' с префиксом 'imol|'.
    """
    if not isinstance(comms, list):
        return None
    for c in comms:
        if (c or {}).get("TYPE") == "IM":
            val = str(c.get("VALUE") or "")
            if val.startswith("imol|"):
                return val
    return None

# === Поиск последних входящих сообщений ===
def fetch_recent_incoming_messages():
    since = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    flt = {
        ">=CREATED": _iso(since),
        "DIRECTION": 2,  # incoming от клиента
    }
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
    return [
        r for r in rows
        if str(r.get("OWNER_TYPE_ID")) in TRACK_ENTITY_TYPES and _is_message_activity(r)
    ]

# === Был ли исходящий ответ после входящего (включая ту же минуту/момент) ===
def has_outgoing_reply_after(entity_type_id, entity_id, t_from_iso: str) -> bool:
    from bitrix import list_activities as _la
    flt = {
        "OWNER_TYPE_ID": int(entity_type_id),
        "OWNER_ID": int(entity_id),
        ">=CREATED": t_from_iso,
        "DIRECTION": 1,
    }
    select = ["ID","CREATED","PROVIDER_ID","PROVIDER_TYPE_ID","AUTHOR_ID"]
    rows = _la(
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
    calls = list_calls_since(
        t_from_iso,
        entity_type_id=int(entity_type_id),
        entity_id=int(entity_id),
        phone=phone
    )
    for c in calls:
        if str(c.get("CALL_FAILED", "N")).upper() != "Y":
            return True

    from bitrix import list_activities as _la
    rows = _la(
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
        if r.get("COMPLETED") == "Y" or str(r.get("DIRECTION", "0")) in ("1", "2"):
            return True
    return False

def communications_first_phone(comms):
    if isinstance(comms, list) and comms:
        v = comms[0].get("VALUE")
        if v:
            return str(v)
    return None

def _last_sender_is_operator_for_openlines(last_activity: dict) -> bool | None:
    """
    Возвращает:
      True  — если последнее сообщение в диалоге написал оператор (менеджер),
      False — если последнее сообщение написал клиент,
      None  — если не удалось определить.
    """
    prov = _as_upper(last_activity.get("PROVIDER_ID"))
    ptype = _as_upper(last_activity.get("PROVIDER_TYPE_ID"))

    if (prov not in OPENLINES_SESSION_IDS) and (ptype not in OPENLINES_SESSION_TYPES):
        return None  # не сессия ОЛ, не обрабатываем тут

    dialog_id = _extract_dialog_id_from_comms(last_activity.get("COMMUNICATIONS"))
    if not dialog_id:
        return None

    msgs = get_last_openlines_messages(dialog_id, limit=1)
    if not msgs:
        return None

    msg = msgs[0]

    # Нормализация: у разных порталов поля разные. Пытаемся определить принадлежность.
    # Критерии (используем первые подходящие):
    # - 'AUTHOR_ID' > 0 и принадлежит сотруднику портала -> оператор
    # - 'AUTHOR_TYPE' in ('operator','bot') -> оператор
    # - 'FROM_USER_ID' == 0 или 'AUTHOR_ID' == 0 -> клиент
    # - 'USER_SOURCE'/'SOURCE' == 'client' -> клиент
    author_type = str(msg.get("AUTHOR_TYPE") or "").lower()
    source      = str(msg.get("SOURCE") or msg.get("USER_SOURCE") or "").lower()
    author_id   = msg.get("AUTHOR_ID")

    if author_type in ("operator", "bot", "system"):
        return True
    if source in ("client", "external", "guest"):
        return False
    if isinstance(author_id, int):
        # Эвристика: в ОЛ у клиента часто AUTHOR_ID == 0
        if author_id == 0:
            return False
        # Иначе считаем это внутренний пользователь (оператор)
        return True

    # Если ничего не распознали
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
        # Парсим дату входящего
        created_raw = str(last.get("CREATED"))
        t_in = _parse_b24_iso(created_raw)

        # Ждём SLA
        if (now_utc - t_in).total_seconds() < RESPONSE_SLA_MIN * 60:
            continue

        # 1) Был ли исходящий ответ после входящего (включая тот же момент)
        if has_outgoing_reply_after(etype, eid, _iso(t_in)):
            continue

        # 2) Для чатов OpenLines/Wazzup: проверяем именно ПОСЛЕДНЕЕ сообщение в диалоге,
        #    а не время закрытия сессии (ключевая логика).
        prov = _as_upper(last.get("PROVIDER_ID"))
        dialog_id = _extract_dialog_id_from_comms(last.get("COMMUNICATIONS"))

        # ЖЁСТКАЯ защита: не ходим в im.dialog.messages.get без валидного dialog_id
        if dialog_id:
            dialog_id_str = str(dialog_id).strip()
        else:
            dialog_id_str = ""

        if dialog_id_str and (prov == "IMOPENLINES_SESSION" or dialog_id_str.startswith("imol|")):
            try:
                lm = _get_last_dialog_message(dialog_id_str)
            except Exception:
                lm = None  # при любой ошибке не валимся, продолжаем обычные проверки

            if lm:
                msg, users = lm
                author_id = int(msg.get("author_id") or msg.get("AUTHOR_ID") or 0)
                # если последнее сообщение от МЕНЕДЖЕРА — тревогу не формируем
                if author_id and _is_user_manager(author_id, users):
                    continue
                # если последнее сообщение от клиента — оставляем кейс на дальнейшие проверки
        # если dialog_id отсутствует или пустой — просто не делаем вызов к im.dialog.messages.get

        # 3) Был ли звонок после входящего (любой успешный)
        phone = communications_first_phone(last.get("COMMUNICATIONS"))
        if has_success_call_after(etype, eid, _iso(t_in), phone):
            continue

        # 4) иначе — тревога
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
