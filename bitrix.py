import os, requests, time

B24_WEBHOOK = os.getenv("B24_WEBHOOK")  # пример: https://xxx.bitrix24.ru/rest/123/xxxxxxxxx/

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

        # если задан лимит — обрежем и выйдем
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

    res = b24("telephony.statistic.get", {"FILTER": flt, "ORDER": {"CALL_START_DATE": "ASC"}, "LIMIT": 100})
    return res.get("result", [])
