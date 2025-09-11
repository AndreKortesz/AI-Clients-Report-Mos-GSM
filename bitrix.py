# bitrix.py
import os, requests, time

B24_WEBHOOK = os.getenv("B24_WEBHOOK")  # https://xxx.bitrix24.ru/rest/123/xxxxxxxxx/

def b24(method: str, params: dict):
    url = f"{B24_WEBHOOK}{method}.json"
    for i in range(3):
        r = requests.post(url, json=params, timeout=30)
        if r.ok:
            data = r.json()
            if 'error' in data:
                raise RuntimeError(f"Bitrix error: {data}")
            return data
        time.sleep(1 + i)
    r.raise_for_status()

def list_activities(filter_, order=None, select=None, start=0, max_rows=None):
    params = {"filter": filter_}
    if order: params["order"] = order
    if select: params["select"] = select
    params["start"] = start

    result = []
    while True:
        res = b24("crm.activity.list", params)
        chunk = res.get("result", []) or []
        result.extend(chunk)

        if max_rows is not None and len(result) >= max_rows:
            return result[:max_rows]

        next_ = res.get("next")
        if not isinstance(next_, int):
            break
        params["start"] = next_
    return result

def list_calls_since(since_iso: str, entity_type_id=None, entity_id=None, phone=None):
    flt = {">=CALL_START_DATE": since_iso}
    if entity_type_id is not None:
        flt["CRM_ENTITY_TYPE"] = entity_type_id
    if entity_id is not None:
        flt["CRM_ENTITY_ID"] = entity_id
    if phone:
        flt["PHONE_NUMBER"] = phone

    res = b24("telephony.statistic.get", {
        "FILTER": flt,
        "ORDER": {"CALL_START_DATE": "ASC"},
        "LIMIT": 100
    })
    return res.get("result", [])

# ---------- НОВОЕ: чтение последних сообщений по диалогу ОЛ ----------
def get_last_openlines_messages(dialog_id: str, limit: int = 1):
    """
    Пытаемся получить последние сообщения диалога открытой линии по dialog_id.
    Возвращает список сообщений (последние сверху).
    Диалог ID мы берём из COMMUNICATIONS[].VALUE, например:
    'imol|wz_whatsapp_...|15|53a5...|35855'  -> целиком передаём как DIALOG_ID.
    """
    # 1) Основной метод Bitrix24
    try:
        res = b24("imopenlines.dialog.messages.get", {
            "DIALOG_ID": dialog_id,
            "LIMIT": limit,
            "ORDER": "DESC"  # последние сначала
        })
        msgs = res.get("result", []) or []
        return msgs
    except Exception as e:
        # 2) Фолбэк: у некоторых порталов доступен другой метод истории
        try:
            res2 = b24("imopenlines.dialog.getHistory", {
                "DIALOG_ID": dialog_id,
                "LIMIT": limit,
                "ORDER": "DESC"
            })
            msgs2 = res2.get("result", []) or []
            return msgs2
        except Exception:
            # 3) Ничего не вышло — отдаём пусто, пусть решает логика наверху
            return []
