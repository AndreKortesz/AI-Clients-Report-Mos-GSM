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

def list_activities(filter_, order=None, select=None, start=0):
    params = {"filter": filter_}
    if order: params["order"] = order
    if select: params["select"] = select
    params["start"] = start
    res = b24("crm.activity.list", params)
    result = res.get("result", [])
    next_ = res.get("next")
    while isinstance(next_, int):
        params["start"] = next_
        res = b24("crm.activity.list", params)
        result.extend(res.get("result", []))
        next_ = res.get("next")
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
