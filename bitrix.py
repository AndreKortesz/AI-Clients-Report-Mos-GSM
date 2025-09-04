import os, requests

B24_WEBHOOK = os.getenv("B24_WEBHOOK", "").rstrip("/") + "/"

def _b24_call(method: str, payload: dict):
    url = f"{B24_WEBHOOK}{method}.json"
    r = requests.post(url, json=payload, timeout=20)
    # Если REST вернул HTTP 200, но в JSON есть error — это тоже ошибка метода/прав
    data = {}
    try:
        r.raise_for_status()
        data = r.json()
    except Exception:
        # попробуем вытащить JSON об ошибке, если он был
        try:
            data = r.json()
        except Exception:
            pass
        raise
    # Вернём и «сырой» ответ, чтобы понять error-код
    return data

def _collect_with_paging(method: str, base_payload: dict) -> list[dict]:
    items = []
    start = 0
    while True:
        payload = dict(base_payload)
        payload["start"] = start
        data = _b24_call(method, payload)
        if isinstance(data, dict) and "error" in data:
            # отдадим наверх — пусть решает вызывающий (сменит метод)
            raise RuntimeError(f"{method}: {data.get('error')} - {data.get('error_description')}")
        chunk = data.get("result", [])
        if not isinstance(chunk, list):
            chunk = []
        items.extend(chunk)
        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start
        if start == 0:
            break
    return items

def list_calls_since(t_from_iso: str, entity_type_id: int | None = None,
                     entity_id: int | None = None, phone: str | None = None) -> list[dict]:
    """
    Возвращает список звонков из журнала телефонии, начиная с t_from_iso.
    Сначала пробуем telephony.statistic.get, если «Method not found» — voximplant.statistic.get.
    Также пробуем оба варианта поля даты: START_DATE / CALL_START_DATE.
    """
    # Базовый фильтр по дате (оба варианта ключа)
    base_filters = [
        {"FILTER": {">=START_DATE": t_from_iso}},
        {"FILTER": {">=CALL_START_DATE": t_from_iso}},
    ]

    # Узкие фильтры (по сущности/телефону) — добавим во все варианты
    def _attach_filters(f: dict) -> dict:
        F = dict(f.get("FILTER", {}))
        if entity_type_id is not None:
            F["CRM_ENTITY_TYPE"] = int(entity_type_id)
        if entity_id is not None:
            F["CRM_ENTITY_ID"] = int(entity_id)
        if phone:
            # по телефону в статистике обычно поле PHONE_NUMBER
            F["PHONE_NUMBER"] = str(phone)
        out = dict(f)
        out["FILTER"] = F
        # сортировка и лимит
        out["ORDER"] = {"START_DATE": "ASC"}
        out["LIMIT"] = 200
        return out

    payloads = [_attach_filters(f) for f in base_filters]

    # Список возможных методов, пробуем по очереди
    methods = ["telephony.statistic.get", "voximplant.statistic.get"]

    last_error = None
    for method in methods:
        for p in payloads:
            try:
                return _collect_with_paging(method, p)
            except RuntimeError as e:
                # Если именно "Method not found" — пробуем следующий метод
                msg = str(e)
                if "ERROR_METHOD_NOT_FOUND" in msg or "Method not found" in msg:
                    last_error = e
                    break  # к следующему методу
                # Другие ошибки (например, прав не хватает) — считаем фатальными для этого метода и пробуем следующий
                last_error = e
                break
            except requests.RequestException as e:
                # сетевые/HTTP ошибки — пробуем следующий вариант
                last_error = e
                break
        else:
            # если не было break — мы уже вернули результат
            pass

    # Если ни один метод не сработал — вернём пусто, чтобы не падал /run-scan
    # (fallback по активностям звонков в logic.py всё равно выполнится)
    return []
