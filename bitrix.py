# bitrix.py
from __future__ import annotations

import os
import time
import typing as t
import requests

# === Базовый URL вебхука ===
_B24 = (os.getenv("B24_WEBHOOK") or "").rstrip("/")
if not _B24:
    raise RuntimeError("Env B24_WEBHOOK is empty")

# === Сетевые таймауты/повторы ===
_HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "25"))
_RETRY = int(os.getenv("HTTP_RETRY", "2"))
_RETRY_SLEEP = float(os.getenv("HTTP_RETRY_SLEEP", "0.8"))

def _method_url(method: str) -> str:
    return f"{_B24}/{method}.json"

def _post(method: str, payload: dict) -> dict:
    """
    Вызов метода Bitrix с базовой обработкой ошибок.
    Любые сетевые/HTTP/битрикс-ошибки -> RuntimeError (для верхнего уровня и фоллбеков).
    """
    url = _method_url(method)
    last_exc: Exception | None = None

    for i in range(_RETRY + 1):
        try:
            r = requests.post(url, json=payload, timeout=_HTTP_TIMEOUT)
            # попробуем разобрать JSON даже при ошибочном статусе
            try:
                data = r.json()
            except ValueError:
                data = None

            if r.status_code >= 400:
                if isinstance(data, dict) and "error" in data:
                    raise RuntimeError(f"{data.get('error')}: {data.get('error_description')}")
                raise RuntimeError(f"HTTP {r.status_code} for {method}")

            if isinstance(data, dict) and "error" in data:
                raise RuntimeError(f"{data.get('error')}: {data.get('error_description')}")

            return data or {}

        except (requests.RequestException, RuntimeError) as e:
            last_exc = e
            if i == _RETRY:
                raise RuntimeError(str(e))
            time.sleep(_RETRY_SLEEP)

    raise RuntimeError(str(last_exc) if last_exc else "Unknown request failure")

# Экспортируем «сырой» вызов как публичный helper
def b24(method: str, params: dict) -> dict:
    return _post(method, params)

# ---------------------------
#  crm.activity.list (постранично)
# ---------------------------
def list_activities(
    flt: dict | None = None,
    order: dict | None = None,
    select: t.List[str] | None = None,
    max_rows: int | None = None,
) -> t.List[dict]:
    method = "crm.activity.list"
    result: t.List[dict] = []
    start: t.Any = 0

    while True:
        payload = {
            "filter": flt or {},
            "order": order or {},
            "select": select or [],
            "start": start,
        }
        data = _post(method, payload)
        page = data.get("result", []) or []
        result.extend(page)

        if max_rows is not None and len(result) >= max_rows:
            return result[:max_rows]

        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start
    return result

# ---------------------------
#  Журнал звонков (телефония) + фоллбек на активности
# ---------------------------
def list_calls_since(
    since_iso: str,
    *,
    entity_type_id: int | None = None,
    entity_id: int | None = None,
    phone: str | None = None,
    max_rows: int | None = 2000,
) -> t.List[dict]:
    """
    Порядок попыток:
      1) voximplant.statistic.get
      2) telephony.statistic.get
      3) crm.activity.list (PROVIDER_ID in ["VOXIMPLANT_CALL","CALL"])
    """
    def _calls_via(method: str) -> t.List[dict]:
        res: t.List[dict] = []
        start = 0
        base_filter = {">=CALL_START_DATE": since_iso}
        if phone:
            base_filter["PHONE_NUMBER"] = phone

        while True:
            payload = {
                "FILTER": base_filter,
                "ORDER": {"CALL_START_DATE": "ASC"},
                "START": start,
            }
            try:
                data = _post(method, payload)
            except Exception as e:
                # Любая ошибка — считаем, что метод недоступен, пробуем следующий
                raise RuntimeError(f"{method} failed: {e}")

            page = data.get("result", []) or []
            res.extend(page)

            if max_rows is not None and len(res) >= max_rows:
                return res[:max_rows]

            next_start = data.get("next")
            if next_start is None:
                break
            start = next_start
        return res

    # 1) и 2)
    try:
        calls = _calls_via("voximplant.statistic.get")
    except RuntimeError:
        try:
            calls = _calls_via("telephony.statistic.get")
        except RuntimeError:
            calls = []

    # Фильтрация по сущности, если указана
    if calls and (entity_type_id or entity_id):
        filtered: t.List[dict] = []
        for c in calls:
            et = str(c.get("CRM_ENTITY_TYPE", c.get("ENTITY_TYPE", "")) or "")
            ei = str(c.get("CRM_ENTITY_ID", c.get("ENTITY_ID", "")) or "")
            if entity_type_id and et and str(entity_type_id) != et:
                continue
            if entity_id and ei and str(entity_id) != ei:
                continue
            filtered.append(c)
        calls = filtered

    # 3) Фоллбек на активности звонков
    if not calls:
        flt = {
            ">CREATED": since_iso,
            "PROVIDER_ID": ["VOXIMPLANT_CALL", "CALL"],
        }
        if entity_type_id is not None:
            flt["OWNER_TYPE_ID"] = int(entity_type_id)
        if entity_id is not None:
            flt["OWNER_ID"] = int(entity_id)

        rows = list_activities(
            flt,
            order={"CREATED": "ASC"},
            select=[
                "ID", "CREATED", "PROVIDER_ID", "DIRECTION",
                "COMPLETED", "SETTINGS", "OWNER_TYPE_ID", "OWNER_ID"
            ],
            max_rows=max_rows or 500,
        )
        calls = [
            {
                "SRC": "crm.activity.list",
                "CREATED": r.get("CREATED"),
                "PROVIDER_ID": r.get("PROVIDER_ID"),
                "DIRECTION": r.get("DIRECTION"),
                "COMPLETED": r.get("COMPLETED"),
                "OWNER_TYPE_ID": r.get("OWNER_TYPE_ID"),
                "OWNER_ID": r.get("OWNER_ID"),
            }
            for r in rows
        ]

    return calls

# ---------------------------
#  OpenLines / Wazzup: чтение последних сообщений диалога
# ---------------------------
def get_last_openlines_messages(dialog_id: str, limit: int = 1) -> dict:
    """
    Обёртка над im.dialog.messages.get.
    Возвращает dict с ключами: {"messages": [...], "users": {...}}
    """
    data = _post("im.dialog.messages.get", {
        "DIALOG_ID": dialog_id,
        "LIMIT": int(limit),
        "SORT": "DESC"
    })
    res = data.get("result") or {}
    # Нормализуем
    return {
        "messages": res.get("messages") or [],
        "users": res.get("users") or {}
    }

def get_last_openlines_message(dialog_id: str) -> tuple[dict, dict] | None:
    """
    Удобный хелпер: одно последнее сообщение и справочник пользователей.
    Возвращает (message, users) или None.
    """
    payload = get_last_openlines_messages(dialog_id, limit=1)
    msgs = payload.get("messages") or []
    if not msgs:
        return None
    return msgs[0], (payload.get("users") or {})

__all__ = [
    "b24",
    "list_activities",
    "list_calls_since",
    "get_last_openlines_messages",
    "get_last_openlines_message",
]
